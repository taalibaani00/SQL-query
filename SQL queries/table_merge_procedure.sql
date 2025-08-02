create
    definer = root@localhost procedure attempt_table_merge(IN source_table_id int, IN min_players_to_start int,
                                                           IN active_server_ids text)
proc_end:
BEGIN
    DECLARE source_lt_id VARCHAR(16);
    DECLARE source_game_id VARCHAR(30);
    DECLARE source_max_seats INT;
    DECLARE source_current_players INT;
    DECLARE target_table_id INT;
    DECLARE target_game_id VARCHAR(30);
    DECLARE target_player_count INT;
    DECLARE merged_ids_str TEXT DEFAULT '';

    -- Start the transaction. All operations must succeed or fail together.
    START TRANSACTION;

    -- ===================================================================
    -- STEP 1: Lock and analyze the source table.
    -- ===================================================================
    SELECT lt_id, game_id, max_seats, (max_seats - seats_available)
    INTO source_lt_id, source_game_id, source_max_seats, source_current_players
    FROM playing_tables
    WHERE table_id = source_table_id
        FOR UPDATE;

    -- If the source table doesn't exist, exit.
    IF source_lt_id IS NULL THEN
        ROLLBACK;
        LEAVE proc_end;
    END IF;

    -- ===================================================================
    -- STEP 2: Loop and merge until the source table is ready to start.
    -- ===================================================================
    merge_loop: LOOP
        -- If we already have enough players, no need to merge further.
        IF source_current_players >= min_players_to_start THEN
            LEAVE merge_loop;
        END IF;

        -- Find the best candidate table to merge.
        -- "Best" is defined as the 'finding' table with the most players
        -- that can fit into our source table.
        SET target_table_id = NULL;
        SELECT
            pt.table_id,
            pt.game_id,
            (pt.max_seats - pt.seats_available)
        INTO target_table_id, target_game_id, target_player_count
        FROM playing_tables pt
        WHERE
            pt.lt_id = source_lt_id          -- Must be the same game type
          AND pt.state = 1                     -- Must be in 'finding' state
          AND pt.table_id != source_table_id -- Cannot merge with itself
          AND FIND_IN_SET(pt.server_id, active_server_ids) > 0 -- Must be on an active server
          AND (pt.max_seats - pt.seats_available) > 0 -- Must have players
          AND (source_current_players + (pt.max_seats - pt.seats_available)) <= source_max_seats -- Combined players must fit
        ORDER BY
            (pt.max_seats - pt.seats_available) DESC, -- Prioritize table with most players
            pt.created_at ASC                        -- Then oldest table
        LIMIT 1
        FOR UPDATE;

        -- If no suitable target was found, we are done.
        IF target_table_id IS NULL THEN
            LEAVE merge_loop;
        END IF;


        -- ===================================================================
        -- STEP 3: Execute the merge for the found target table.
        -- This involves two steps as per the schema of 'table_game_user_map':
        -- 1. Create new player assignments on the source table.
        -- 2. Shadow the old player assignments on the target table.
        -- ===================================================================

        -- Step 3.1: Create new entries for the moving players, assigning them
        -- to the source table with its game_id. We do this first to ensure
        -- the players are safely moved before their old records are touched.
        INSERT INTO table_game_user_map (table_id, game_id, user_id,reason, created_by)
        SELECT
            source_table_id,      -- The new table_id
            source_game_id,       -- The source table's game_id
            user_id,                -- The user_id from the target table
            'table_merge',   -- reason
            'table_merge_system'    -- created_by
        FROM table_game_user_map
        WHERE table_id = target_table_id AND game_id = target_game_id AND is_shadowed = 0;

        -- Step 3.2: Mark the old assignments on the target table as shadowed.
        UPDATE table_game_user_map
        SET
            is_shadowed = 1,
            reason = 'table_merged',
            updated_by = 'table_merge_system'
        WHERE
            table_id = target_table_id AND game_id = target_game_id AND is_shadowed = 0;

        -- Mark the target table as 'finished' or 'merged'.
        -- Using state 6 for 'merged'.
        UPDATE playing_tables
        SET state = 6, seats_available = max_seats
        WHERE table_id = target_table_id;

        -- Update the source table's seat count.
        SET source_current_players = source_current_players + target_player_count;
        UPDATE playing_tables
        SET seats_available = seats_available - target_player_count
        WHERE table_id = source_table_id;

        -- Record the ID of the table we just merged, ensuring correct comma placement.
        IF merged_ids_str = '' THEN
            SET merged_ids_str = target_table_id;
        ELSE
            SET merged_ids_str = CONCAT(merged_ids_str, ',', target_table_id);
        END IF;

    END LOOP merge_loop;

    -- Commit all changes.
    COMMIT;

    -- ===================================================================
    -- STEP 5: Return the list of merged table IDs as a string.
    -- ===================================================================
    SELECT merged_ids_str AS merged_table_ids;

END;

