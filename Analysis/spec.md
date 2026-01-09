Er moet een Python script gemaakt worden dat, wanneer het wordt uitgevoerd, een overzicht maakt van de keuringsinfo.

De keuringsinfo zit op LSDeel of LS assets. Deze worden **alleen** als een paar beschouwd wanneer er een **Voedt**-relatie bestaat van **LS -> LSDeel**.
Er wordt **geen** fallback gedaan op basis van naampad of parent matching.

Indien info op beide assets zit, dan mag deze info geconsolideerd worden, waarbij de meest recent info wint.
De keuringsinfo zit als volgt gedocumenteerd:
  "ins": {
    "EMObject_resultaatKeuring": "niet-conform met inbreuken",
    "EMObject_datumLaatsteKeuring": "2020-10-16"
  }

De data moet opgehaald worden via AQL queries en als resultaat in een Excel bestand komen.
Per toezichtgroep moet er een lijst zijn die in een aparte sheet staat.
De toezichtgroepen zijn: V&W-WL, V&W-WA, V&W-WO, V&W-WW, V&W-WVB.
Alle andere assets mogen met hun toezichtgroep in een "Andere" categorie geplaatst worden en in een apart tabblad
Om de data te accesen, gebruik je de AranagoDBConnectionFactory, met dezelfde settings als in main_linux_aranago.py


zie ook aanpak.md








voorbeeldquery's:
```aql
LET stroomkring_key        = FIRST(FOR at IN assettypes FILTER at.short_uri == "onderdeel#Stroomkring"        LIMIT 1 RETURN at._key)
LET laagspanningsbord_key  = FIRST(FOR at IN assettypes FILTER at.short_uri == "onderdeel#Laagspanningsbord"  LIMIT 1 RETURN at._key)
LET lsdeel_key             = FIRST(FOR at IN assettypes FILTER at.short_uri == "lgc:installatie#LSDeel"          LIMIT 1 RETURN at._key)
LET hoortbij_key           = FIRST(FOR rt IN relatietypes FILTER rt.short == "HoortBij"                      LIMIT 1 RETURN rt._key)

FOR s IN assets
  FILTER
    s.AIMDBStatus_isActief == true
    AND (
      s.assettype_key == stroomkring_key
      OR s.assettype_key == laagspanningsbord_key
    )

  LET lsdeel = FIRST(
    FOR l, rel IN OUTBOUND s assetrelaties
      FILTER
        rel.relatietype_key == hoortbij_key
        AND l.assettype_key == lsdeel_key
        AND l.AIMDBStatus_isActief == true
      LIMIT 1
      RETURN l
  )
  FILTER lsdeel == null

  RETURN {
    uuid: s._key,
    naam: s.AIMNaamObject_naam
  }
```

```aql
LET camera_key = FIRST(FOR at IN assettypes FILTER at.short_uri == "onderdeel#Camera" LIMIT 1 RETURN at._key)

FOR c IN assets
  FILTER
    c.assettype_key == camera_key
    AND c.AIMDBStatus_isActief == true

  LET toezichter = FIRST(
    FOR v, e IN 1..1 OUTBOUND c._id betrokkenerelaties
      FILTER e.rol == "toezichter"
      RETURN v
  )

  FILTER toezichter == null

  RETURN {
    uuid: c._key,
    naam: c.AIMNaamObject_naam
  }
```