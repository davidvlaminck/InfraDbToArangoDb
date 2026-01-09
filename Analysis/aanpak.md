# Keuring laagspanningsborden

Het is noodzakelijk de keuringen van laagspanningsborden op te volgen. 
De data in Infra DB zal gebruikt worden om een analyse te maken en hierover te rapporteren.
Dit document beschrijft welke aannames worden gedaan en hoe de data wordt gebruikt.

 ## Assettype
Een laagspanningsbord wordt in de OTL als een object van het type "Laagspanningsbord" gemodelleerd. 
Begin 2026 is deze data echter nog niet volledig beschikbaar in Infra DB. 
Daarom wordt voorlopig gebruik gemaakt van de legacy types "LS", "LSBord" en "LSDeel". 
Deze data worden in 2026 gemigreerd (verweven) naar de OTL conforme versie. 
Daarbij gaan de relevante attributen op de legacy types op het OTL conforme assettype Laagspanningsbord worden overgezet.

## Dubbele data
Aangezien de keuringsdata zowel op LS als LSDeel kan aanwezig zijn, moeten we deze data consolideren.
Wanneer een LSDeel voeding krijgt van een LS, kan de keuringsdata als één worden beschouwd.
In dat geval wordt de keuringsdatum gebruikt en behouden we de meest recente data.
Als de voedingsrelatie ontbreekt, is het voldoende dat de LS en LSDeel dezelfde parent hebben in de boomstructuur.
Dat wil zeggen dat het naampad gelijk is, behalve het laatste deel.

## Te negeren data
Er zijn laagspanningsborden aanwezig in de databank die we kunnen negeren voor het rapport.
Dit zijn o.a. assets waarvan de toestand op verwijderd of overgedragen staat.

## Indelen in groepen
Legacy data heeft een toezichtgroep attribuut dat kan worden gebruikt om de borden in groepen in te delen.
OTL data heeft een betrokkenerelatie met als rol toezichtgroep.
De assets die deze data ontbreken worden in een aparte lijst getoond, zodat die eventueel kan worden aangevuld.

