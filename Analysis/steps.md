# Volgorde uit te voeren stappen

## 1. Verweving migratie
Haal het resultaat op van volgende query (in json) en sla deze op als migration_LSDeel.json.
```
LET at_key = FIRST(FOR at IN assettypes FILTER at.short_uri == 'onderdeel#Laagspanningsbord' LIMIT 1 RETURN at._key)
LET lsdeel_key = FIRST(FOR at IN assettypes FILTER at.short_uri == 'lgc:installatie#LSDeel' LIMIT 1 RETURN at._key)
LET key_rel = FIRST(FOR rt IN relatietypes FILTER rt.naam == "GemigreerdNaar" LIMIT 1 RETURN rt._key)

FOR a IN assets
  FILTER a.AIMDBStatus_isActief == true
  FILTER a.assettype_key == at_key

  LET migrated_from_list = (
    FOR v, e IN 1..1 INBOUND a assetrelaties
      FILTER e.relatietype_key == key_rel
      FILTER v.assettype_key == lsdeel_key
      LIMIT 1
      RETURN v._key
  )

  RETURN {
    uuid: a._key,
    migrated_from_uuids: migrated_from_list[0]
  }
```

