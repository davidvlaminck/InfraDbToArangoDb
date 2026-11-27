# Technieken Attribute Population for Root Installation Assets

## Overview
The `technieken` attribute is added to root installation assets (type `Instal`) after they are created. This attribute is an array of strings that indicates the technical categories applicable to each installation.

## Population Process

### Step 1: Create Root Assets
Root assets are created via `create_instal_root_assets.aql`. Each unique root name from `assets.naampad_parts[0]` becomes an `Instal` asset with a deterministic `_key` (`instal_<rootName>` with slashes replaced by underscores).

### Step 2: Update with Technieken
The `update_instal_root_technieken.aql` query runs after root creation and performs:

1. **Select all root installation assets** - Filter by `@type` = `https://lgc.data.wegenenverkeer.be/ns/installatie#Instal`

2. **Initialize technieken array** - Every root asset receives a `technieken` attribute set to an empty array `[]`

3. **Conditional append for TUNNEL** - For each asset, check if `NaampadObject_naampad` contains "TUNNEL" (case-insensitive):
   - If true: set `technieken` to `["Tunnel"]`
   - If false: keep `technieken` as `[]`

## AQL Logic
```aql
LET hasTunnel = LIKE(a.NaampadObject_naampad, "%TUNNEL%", true)
LET technieken = hasTunnel ? ["Tunnel"] : []
UPDATE a WITH { technieken: technieken } IN assets
```

- `LIKE(..., "%TUNNEL%", true)` - Case-insensitive pattern match
- Ternary operator `? :` sets the array value conditionally

## Result
- Root installations with "TUNNEL" in their naampad → `technieken: ["Tunnel"]`
- All other root installations → `technieken: []`

## Future Extensions
Additional techniques can be added by extending the conditional logic:
```aql
LET technieken = []
LET technieken = PUSH(technieken, "Tunnel") // if TUNNEL in name
LET technieken = PUSH(technieken, "Brug")   // if BRUG in name
// etc.
```