# Cattle Use Case - BAROMETER
...

## Authenticating with Tableau Cloud
The library provides two methods of providing authentication credentials for
connecting to Tableau Cloud:
1. Through parameters specified in-code when creating the Tableau object.
2. Through a JSON configuration file, which is the passen to the Tableau object.

Though the first method can be useful when using the library locally, the
second method is preferred when running this library on Azure Functions. Below
is an example provided of the JSON configuration file, named `tableau_conf.json`:
```json
{
  "site_url": "https://eu-west-1a.online.tableau.com",
  "username": "<YOUR USERNAME>",
  "password": "<YOUR PASSWORD>",
  "site_name": "bovianalytics"
}
```
This configuration file can then be passed to the Tableau object as follows:
```python
from barometer.tableau import Tableau

tableau = Tableau.from_conf("tableau_conf.json")
```
