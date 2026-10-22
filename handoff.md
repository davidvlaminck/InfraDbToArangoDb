# Handoff: Cookie-authenticatie voor de EMInfra API

## Doel

Ondersteunen van cookie-gebaseerde authenticatie bij de EMInfra API, zodat een gebruiker een cookie uit zijn browser kan gebruiken in plaats van een certificaat of JWT. Dit is relevant wanneer men geen toegang heeft tot een client-certificaat of private key, maar wel ingelogd is in de EMInfra portal via een browser.

## Huidige staat (al geïmplementeerd)

De onderstaande onderdelen bestaan al in de repo:

| Bestand | Regel | Inhoud |
|---------|-------|--------|
| `API/APIEnums.py` | 18 | `AuthType.COOKIE = 'cookie'` |
| `API/CookieRequester.py` | 6-40 | `CookieRequester` die `Cookie: acm-awv=<cookie>` meestuurt met elk verzoek |
| `API/RequesterFactory.py` | 30-38 | `create_requester` accepteert `auth_type=AuthType.COOKIE` en een `cookie`-argument |
| `API/EMInfraClient.py` | 20-21 | Accepteert `cookie`-parameter en geeft deze door aan de factory |
| `API/EMSONClient.py` | 29-30 | Idem als `EMInfraClient` |

De cookie wordt verstuurd als header `Cookie: acm-awv=<waarde>`.

## Wat nog ontbreekt

De volgende stappen moeten nog uitgevoerd worden om cookie-authentificatie bruikbaar te maken in de pipeline:

1. **`DBPipelineController`** (`DBPipelineController.py:21`)
   - Voeg een `cookie: str | None = None` parameter toe aan `__init__`.
   - Geef deze door aan `EMInfraClient` en `EMSONClient` (zie `settings_to_clients`).

2. **Entrypoint-scripts** (`main_linux_arango.py`, `main_windows_arango.py`)
   - Breid uit zodat de gebruiker kan kiezen tussen `AuthType.JWT`, `AuthType.CERT` en `AuthType.COOKIE`.
   - Bij `AuthType.COOKIE` moet een cookie-waarde meegegeven worden (via argument, env var of input prompt).

3. **Cookie-extractie uit browser** (optioneel, aanbevolen)
   - Er is nog geen library geïnstalleerd om cookies programmatisch uit een browser te lezen.
   - Aanbevolen: `browser_cookie3` (`pip install browser_cookie3`), toevoegen aan `requirements.txt`.
   - Voorbeeldgebruik:
     ```python
     import browser_cookie3
     cookies = browser_cookie3.chrome(domain_name='apps.mow.vlaanderen.be')
     cookie_value = [c.value for c in cookies if c.name == 'acm-awv'][0]
     ```

4. **Documentatie**
   - Voeg cookie-authenticatie toe aan `README.md` (installatie + gebruik).
   - Werk `settings_sample.json` zo nodig bij (cookie heeft geen `settings`-blok nodig, in tegenstelling tot JWT/CERT).

5. **Tests**
   - Voeg een test toe voor `CookieRequester` en/of de cookie-auth flow via `RequesterFactory`.

## Hoe de cookie te gebruiken (code-voorbeelden)

### Via `EMInfraClient` of `EMSONClient`

```python
from API.APIEnums import AuthType, Environment
from API.EMInfraClient import EMInfraClient

client = EMInfraClient(
    auth_type=AuthType.COOKIE,
    env=Environment.PRD,
    cookie="JE_COOKIE_WAARDE_HIER"
)
```

### Via `RequesterFactory`

```python
from API.APIEnums import AuthType, Environment
from API.RequesterFactory import RequesterFactory

requester = RequesterFactory.create_requester(
    auth_type=AuthType.COOKIE,
    env=Environment.PRD,
    cookie="JE_COOKIE_WAARDE_HIER"
)
```

### Via `DBPipelineController` (na aanpassing)

```python
controller = DBPipelineController(
    settings_path=settings_path,
    auth_type=AuthType.COOKIE,
    env=Environment.PRD,
    cookie="JE_COOKIE_WAARDE_HIER"
)
```

## Cookie ophalen uit de browser

1. Open je browser en log in op de EMInfra omgeving (bijv. `https://services.apps.mow.vlaanderen.be`).
2. Open DevTools → Application → Cookies.
3. Zoek de cookie met de naam `acm-awv`.
4. Kopieer de waarde van die cookie.
5. Geef deze waarde door als `cookie`-argument (zie boven).

## Belangrijke aandachtspunten

- **Cookies verlopen.** Bij een 401/403 response moet de gebruiker een verse cookie uit de browser halen.
- `CookieRequester` zet standaard de headers `Content-Type: application/vnd.awv.eminfra.v1+json` en `accept: application/json`.
- Voor cookie-auth wordt de `first_part_url` aangepast: `services.` wordt verwijderd uit de domeinnaam (zie `RequesterFactory.py:38`).
- Cookie-auth heeft geen `settings`-blok nodig (anders dan JWT en CERT).
