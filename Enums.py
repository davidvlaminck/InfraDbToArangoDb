
from enum import Enum, unique

import colorama


class DBStep(Enum):
    CREATE_DB          = "0_create_db"           # Set up the initial database
    INITIAL_FILL       = "1_initial_fill"        # Populate the DB with base data
    EXTRA_DATA_FILL    = "2_extra_data_fill"     # Add supplementary/derived data
    CREATE_INDEXES     = "3_create_indexes"      # Build indexes for performance
    APPLY_CONSTRAINTS  = "4_apply_constraints"   # Enforce schema/data rules
    FINAL_SYNC         = "5_final_sync"          # Sync with external systems


@unique
class ResourceEnum(str, Enum):
    agents = 'agents'
    bestekken = 'bestekken'
    toezichtgroepen = 'toezichtgroepen'
    identiteiten = 'identiteiten'
    relatietypes = 'relatietypes'
    assettypes = 'assettypes'
    beheerders = 'beheerders'
    betrokkenerelaties = 'betrokkenerelaties'
    assetrelaties = 'assetrelaties'
    assets = 'assets'
    controlefiches = 'controlefiches'


colorama_table = {
    ResourceEnum.assets: colorama.Fore.GREEN,
    ResourceEnum.agents: colorama.Fore.YELLOW,
    ResourceEnum.assetrelaties: colorama.Fore.CYAN,
    ResourceEnum.betrokkenerelaties: colorama.Fore.MAGENTA,
    ResourceEnum.controlefiches: colorama.Fore.LIGHTBLUE_EX,
    ResourceEnum.bestekken: colorama.Fore.BLUE,
    ResourceEnum.toezichtgroepen: colorama.Fore.LIGHTYELLOW_EX,
    ResourceEnum.identiteiten: colorama.Fore.LIGHTCYAN_EX,
    ResourceEnum.relatietypes: colorama.Fore.LIGHTGREEN_EX,
    ResourceEnum.assettypes: colorama.Fore.LIGHTMAGENTA_EX,
    ResourceEnum.beheerders: colorama.Fore.LIGHTRED_EX
}