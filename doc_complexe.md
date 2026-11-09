# Documentation — DAGs Airflow 3 avec SSH

## 1. Objectif

- **Chaîne 1** : A à partir de 1 h → B à partir de 2 h si A réussit → C à partir de 3 h si B réussit.
- **Chaîne 2** : D lorsqu’un fichier CSV est présent sur le serveur distant.
- **Exclusion** : les deux chaînes ne doivent jamais tourner simultanément.

Les horaires sont des heures minimales, dans le fuseau **Europe/Paris**.

## 2. Imports et constantes

| Ligne | Explication |
|---|---|
| `from datetime import timedelta` | Permet de définir une durée, ici 23 heures. |
| `import pendulum` | Gère les dates et les fuseaux horaires. |
| `from airflow.sdk import dag, task` | Importe les décorateurs Airflow 3. |
| `from ... import SSHHook` | Permet d’ouvrir une connexion SSH dans du code Python. |
| `from ... import SSHOperator` | Permet d’exécuter une commande distante comme tâche Airflow. |
| `from ... import DateTimeSensor` | Attend une date et une heure précises. |
| `from ... import TriggerDagRunOperator` | Déclenche un autre DAG et peut attendre sa fin. |
| `SSH_CONN = "serveur_ssh"` | Identifiant de la connexion SSH configurée dans Airflow. |
| `POOL = "chaines_exclusives"` | Nom du pool partagé qui sert de verrou. |
| `START = pendulum.datetime(...)` | Date de départ du planning, dans le fuseau de Paris. |

## 3. Déclaration des DAGs

Ces paramètres sont communs aux trois DAGs :

| Ligne | Explication |
|---|---|
| `@dag(...)` | Transforme la fonction qui suit en définition de DAG. |
| `start_date=START` | Définit le début du planning ; ne déclenche pas immédiatement le DAG. |
| `schedule=None` | Désactive la planification automatique des chaînes 1 et 2. |
| `schedule="0 0 * * *"` | Planifie le pilotage chaque jour à minuit, heure de Paris. |
| `catchup=False` | Évite de créer toutes les exécutions historiques manquées. |
| `max_active_runs=1` | Autorise une seule exécution active de ce DAG. |
| `def chaine_1():` | Définit la chaîne 1 ; son nom devient l’identifiant du DAG. |
| `def chaine_2():` | Définit la chaîne 2. |
| `def pilotage_chaines():` | Définit le DAG qui pilote les deux chaînes. |

**Attention :** `max_active_runs=1` ne bloque pas deux DAGs différents.
C’est le pool partagé qui assure leur exclusion.

## 4. Jobs SSH : A, B, C et D

| Ligne | Explication |
|---|---|
| `a = SSHOperator(...)` | Crée une tâche SSH et garde sa référence dans `a`. |
| `task_id="Job_A"` | Nom de la tâche dans l’interface Airflow. |
| `ssh_conn_id=SSH_CONN` | Utilise la connexion SSH `serveur_ssh`. |
| `command="/opt/jobs/job_a.sh"` | Exécute ce script sur le serveur distant. |
| `cmd_timeout=None` | Désactive la limite de durée de la commande SSH. |

Les tâches B, C et D utilisent le même principe, avec leur propre script.

Un script doit :
- retourner **0** en cas de succès ;
- retourner un code différent en cas d’échec ;
- attendre la fin réelle du traitement, sans lancement en arrière-plan.

## 5. Attente des horaires

| Ligne | Explication |
|---|---|
| `attendre_2h = DateTimeSensor(...)` | Crée une tâche qui attend 2 h. |
| `task_id="attendre_2h"` | Nom de cette attente dans Airflow. |
| `target_time="{{ dag_run.conf['heure_2'] }}"` | Lit l’heure cible transmise par le pilotage. |
| `mode="reschedule"` | Libère le worker entre deux vérifications. |
| `poke_interval=30` | Vérifie toutes les 30 secondes. |

`attendre_3h` fonctionne de la même manière avec `heure_3`.

Les expressions `{{ ... }}` sont des templates **Jinja** :
Airflow les remplace par leur valeur lors de l’exécution.

### Dépendances de la chaîne 1

`a >> attendre_2h >> b >> attendre_3h >> c`

Le symbole `>>` signifie « doit précéder ».

Par défaut, chaque tâche exige la réussite de la précédente :
- A échoue → B et C ne s’exécutent pas ;
- A finit avant 2 h → B attend 2 h ;
- A finit après 2 h → B peut démarrer après sa réussite ;
- B échoue → C ne s’exécute pas.

## 6. Vérification du fichier distant

| Ligne | Explication |
|---|---|
| `@task.sensor(...)` | Transforme la fonction suivante en tâche de surveillance. |
| `poke_interval=60` | Vérifie le répertoire toutes les minutes. |
| `timeout=timedelta(hours=23)` | Limite l’attente à 23 heures. |
| `mode="reschedule"` | Libère le worker entre les vérifications. |
| `soft_fail=True` | Marque la tâche comme ignorée si le délai expire. |
| `def attendre_fichier():` | Définit la vérification du fichier. |
| `with SSHHook(...).get_conn() as client:` | Ouvre la connexion SSH et la ferme en sortant du bloc. |
| `client.exec_command(...)` | Exécute la commande de recherche à distance. |
| `_, stdout, stderr = ...` | Ignore l’entrée standard et récupère les sorties normale et d’erreur. |
| `stdout.read().decode().strip()` | Lit le résultat, le convertit en texte et retire les espaces autour. |
| `stderr.read().decode()` | Lit les éventuelles erreurs. |
| `stdout.channel.recv_exit_status()` | Récupère le code de retour de la commande. |
| `if code != 0:` | Détecte un échec de la vérification. |
| `raise RuntimeError(...)` | Fait échouer la tâche si la commande SSH échoue. |
| `return bool(resultat)` | Renvoie vrai si un fichier a été trouvé ; sinon le sensor attend encore. |

La commande distante est :

`find /data/entree -maxdepth 1 -type f -name '*.csv' -print -quit`

- `/data/entree` : répertoire à surveiller.
- `-maxdepth 1` : ne parcourt pas les sous-répertoires.
- `-type f` : recherche seulement des fichiers.
- `-name '*.csv'` : sélectionne les fichiers CSV.
- `-print -quit` : affiche le premier fichier trouvé puis s’arrête.

## 7. Pilotage et verrou commun

### Heure de démarrage

`attendre_1h` utilise un `DateTimeSensor` avant de déclencher la chaîne 1.

Son template :
- prend `data_interval_end`, la fin de l’intervalle planifié ;
- convertit cette date en heure de Paris ;
- remplace l’heure par 1 h ;
- utilise `isoformat()` pour produire une date lisible par le sensor.

Les valeurs `heure_2` et `heure_3` sont calculées de la même façon.

### Déclencheurs

| Ligne | Explication |
|---|---|
| `lancer_1 = TriggerDagRunOperator(...)` | Crée la tâche qui lance la chaîne 1. |
| `task_id="lancer_chaine_1"` | Nom du déclencheur dans le pilotage. |
| `trigger_dag_id="chaine_1"` | Identifiant du DAG à lancer. |
| `conf={...}` | Transmet les heures de 2 h et 3 h au DAG enfant. |
| `pool=POOL` | Réserve l’unique slot du pool partagé. |
| `wait_for_completion=True` | Attend la fin du DAG enfant. |
| `deferrable=True` | Libère le worker pendant cette attente, grâce au Triggerer. |
| `poke_interval=30` | Intervalle de vérification de la fin du DAG enfant. |

`lancer_2` applique le même verrou pour la chaîne 2.

**Le pool doit compter les tâches différées**, sinon le slot serait libéré pendant l’attente et l’exclusion ne serait plus garantie.

### Dépendances du pilotage

- `attendre_1h >> lancer_1` : attend 1 h avant de demander le verrou et de lancer A.
- `attendre_fichier() >> lancer_2` : attend un fichier avant de demander le verrou et de lancer D.

La recherche du fichier peut se faire pendant la chaîne 1, mais **le traitement D reste bloqué par le pool**.

## 8. Enregistrement final

- `chaine_1()` : instancie le premier DAG.
- `chaine_2()` : instancie le deuxième DAG.
- `pilotage_chaines()` : instancie le DAG de pilotage.

Ces appels créent les DAGs pour Airflow ; ils ne lancent pas les scripts SSH immédiatement.

## 9. Précautions

- Activer les trois DAGs et le **Triggerer**.
- Créer `chaines_exclusives` avec **1 slot** et **Include deferred tasks activé**.
- Lancer les chaînes uniquement via le pilotage.
- Ne pas affecter les jobs SSH au pool partagé : le déclencheur occupe déjà son slot.
- Ne pas réinitialiser un déclencheur pendant que son DAG enfant tourne.
- Dans `job_d.sh`, revérifier les fichiers et les archiver après succès.

**Limite :** D est lancé au maximum une fois par exécution quotidienne du pilotage. Si cette exécution dure plus d’une journée, elle retarde la suivante.