from enum import Enum


class DBStep(Enum):
    INITIAL_FILL       = "1_initial_fill"        # Populate the DB with base data
    EXTRA_DATA_FILL    = "2_extra_data_fill"     # Add supplementary/derived data
    CREATE_INDEXES     = "3_create_indexes"      # Build indexes for performance
    APPLY_CONSTRAINTS  = "4_apply_constraints"   # Enforce schema/data rules
    FINAL_SYNC         = "5_final_sync"          # Sync with external systems