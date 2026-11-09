# Documentation — Chaînes Airflow avec BashOperator

## 1. Logique générale

Le fichier Python définit trois DAGs :

| DAG | Rôle |
|---|---|
| `chaine1` | Exécuter A, puis B, puis C, avec des heures minimales. |
| `chaine2` | Exécuter D pour traiter les fichiers disponibles. |
| `pilotage` | Attendre les conditions, puis déclencher les chaînes avec un verrou commun. |

### Chaîne 1

A à partir de 1 h → attendre 2 h → B → attendre 3 h → C.

- B ne démarre que si A réussit.
- C ne démarre que si B réussit.
- Si A finit après 2 h, B démarre après sa réussite.
- Si B finit après 3 h, C démarre après sa réussite.

### Chaîne 2

Attendre un fichier CSV → lancer D.

Le fichier est recherché dans `/data/entree/`, accessible depuis les workers Airflow.

### Exclusion des chaînes

Les deux déclencheurs utilisent le pool `chaines_exclusives`, limité à un slot.
Celui qui obtient le slot le conserve jusqu’à la fin de sa chaîne.

Ainsi, D ne peut pas tourner pendant A, B ou C, ni pendant les attentes entre ces jobs.

## 2. Imports et constantes

| Ligne | Explication |
|---|---|
| `import pendulum` | Importe la bibliothèque de gestion des dates et fuseaux horaires. |
| `from airflow.sdk import dag` | Importe le décorateur qui permet de définir les DAGs en Airflow 3. |
| `from ...operators.bash import BashOperator` | Importe l’opérateur qui exécute une commande Bash sur le worker. |
| `from ... import TriggerDagRunOperator` | Importe l’opérateur qui déclenche un autre DAG. |
| `from ... import DateTimeSensor` | Importe le sensor qui attend une date et une heure. |
| `from ... import FileSensor` | Importe le sensor qui attend un fichier accessible depuis le worker. |
| `POOL = "chaines_exclusives"` | Définit le nom du pool utilisé comme verrou. |
| `START = pendulum.datetime(2025, 1, 1, tz="Europe/Paris")` | Définit la date de départ du planning et son fuseau horaire. |

Les parenthèses autour d’un import permettent simplement de l’écrire sur plusieurs lignes.

## 3. Déclaration des DAGs

Les trois DAGs utilisent le décorateur `@dag(...)`.

| Ligne | Explication |
|---|---|
| `@dag(...)` | Transforme la fonction suivante en définition de DAG. |
| `dag_id="chaine1"` | Identifiant affiché dans Airflow et utilisé pour déclencher ce DAG. |
| `dag_id="chaine2"` | Identifiant du DAG de traitement D. |
| `dag_id="pilotage"` | Identifiant du DAG de pilotage. |
| `start_date=START` | Fixe le début du planning ; ne lance pas immédiatement les tâches. |
| `schedule=None` | Désactive la planification automatique des chaînes 1 et 2. |
| `schedule="0 0 * * *"` | Planifie le pilotage chaque jour à minuit, heure de Paris. |
| `catchup=False` | Évite de rejouer toutes les périodes historiques manquées. |
| `max_active_runs=1` | Limite chaque DAG à une seule exécution active. |
| `def chaine_1():` | Définit les tâches de la chaîne 1. |
| `def chaine_2():` | Définit les tâches de la chaîne 2. |
| `def pilotage_chaines():` | Définit les attentes et les déclencheurs. |

**Important :** `max_active_runs=1` s’applique séparément à chaque DAG.
Ce paramètre ne garantit donc pas, à lui seul, l’exclusion entre deux chaînes.

## 4. Exécution des jobs avec BashOperator

Exemple pour A :

`a = BashOperator(...)`

| Ligne | Explication |
|---|---|
| `a = BashOperator(...)` | Crée une tâche Bash et conserve sa référence dans la variable `a`. |
| `task_id="Job_A"` | Définit le nom de la tâche dans Airflow. |
| `bash_command="bash /opt/jobs/job_a.sh "` | Lance le script avec Bash sur le worker qui exécute la tâche. |
| `skip_on_exit_code=None` | Tout code de sortie différent de 0 fait échouer la tâche. |

B, C et D suivent le même principe :

| Tâche | Script |
|---|---|
| `Job_A` | `/opt/jobs/job_a.sh` |
| `Job_B` | `/opt/jobs/job_b.sh` |
| `Job_C` | `/opt/jobs/job_c.sh` |
| `Job_D` | `/opt/jobs/job_d.sh` |

L’espace à la fin des commandes évite qu’Airflow traite leur suffixe `.sh`
comme un fichier template Jinja à charger.

**BashOperator n’exécute pas ces scripts sur un serveur distant.**
Les scripts doivent être présents et lisibles dans l’environnement du worker.

## 5. Attentes de 2 h et 3 h

| Ligne | Explication |
|---|---|
| `attendre_2h = DateTimeSensor(...)` | Crée l’attente de 2 h. |
| `task_id="attendre_2h"` | Nom de cette tâche dans Airflow. |
| `target_time="{{ dag_run.conf['heure_2'] }}"` | Lit l’heure cible transmise par le pilotage au lancement du DAG. |
| `mode="reschedule"` | Libère le worker entre deux vérifications. |
| `poke_interval=30` | Vérifie toutes les 30 secondes si l’heure cible est atteinte. |

`attendre_3h` fonctionne de la même façon, avec `heure_3`.

Les expressions `{{ ... }}` sont des templates Jinja :
Airflow les remplace par leurs valeurs pendant l’exécution.

Si l’heure cible est déjà passée, le sensor peut réussir dès sa vérification.

### Ordre des tâches

`a >> attendre_2h >> b >> attendre_3h >> c`

Le symbole `>>` définit une dépendance entre les tâches.

Par défaut, une tâche ne s’exécute que si ses prédécesseurs ont réussi.
Un échec de A empêche donc B et C de s’exécuter ; un échec de B empêche C.

## 6. Attente de 1 h dans le pilotage

| Ligne | Explication |
|---|---|
| `attendre_1h = DateTimeSensor(...)` | Attend l’heure minimale de lancement de la chaîne 1. |
| `task_id="attendre_1h"` | Nom de l’attente dans Airflow. |
| `target_time=(...)` | Contient le template qui calcule la date cible. |
| `data_interval_end` | Date de fin de l’intervalle couvert par l’exécution planifiée. |
| `.in_timezone('Europe/Paris')` | Convertit cette date dans le fuseau de Paris. |
| `.replace(hour=1, minute=0, second=0)` | Fixe l’heure cible à 1 h pour cette date. |
| `.isoformat()` | Convertit la date en texte au format ISO. |
| `mode="reschedule"` | Libère le worker pendant l’attente. |
| `poke_interval=30` | Vérifie l’heure toutes les 30 secondes. |

Les chaînes de caractères adjacentes du template sont assemblées automatiquement par Python.

## 7. Surveillance des fichiers avec FileSensor

| Ligne | Explication |
|---|---|
| `attendre_fichier = FileSensor(...)` | Crée une tâche qui attend un fichier CSV. |
| `task_id="attendre_fichier"` | Nom de la tâche. |
| `fs_conn_id="fs_default"` | Utilise la connexion Airflow de type File (path). |
| `filepath="/data/entree/*.csv"` | Recherche les fichiers correspondant au motif `*.csv` dans ce répertoire. |
| `mode="reschedule"` | Libère le worker entre les vérifications. |
| `poke_interval=60` | Vérifie la présence du fichier toutes les minutes. |
| `timeout=23 * 3600` | Limite l’attente à 23 heures, exprimées en secondes. |
| `soft_fail=True` | Si le délai expire, marque le sensor comme ignoré plutôt qu’en échec. |

Si le sensor est ignoré, le déclencheur de la chaîne 2 est également ignoré
avec sa règle de déclenchement par défaut.

**Attention :**
- Le répertoire doit être accessible depuis tous les workers susceptibles d’exécuter le sensor.
- Avec Docker ou Kubernetes, il doit être monté dans les conteneurs ou pods.
- Un fichier présent n’est pas forcément entièrement écrit.

Idéalement, le producteur écrit sous un nom temporaire, puis renomme le fichier
en `.csv` une fois l’écriture terminée.

## 8. Déclenchement de la chaîne 1

| Ligne | Explication |
|---|---|
| `lancer_1 = TriggerDagRunOperator(...)` | Crée la tâche qui déclenche la chaîne 1. |
| `task_id="lancer_chaine_1"` | Nom du déclencheur dans le pilotage. |
| `trigger_dag_id="chaine1"` | Désigne le DAG à lancer ; doit correspondre exactement à son `dag_id`. |
| `conf={...}` | Transmet des paramètres au DAG enfant. |
| `"heure_2": (...)` | Calcule et transmet l’heure cible de 2 h. |
| `"heure_3": (...)` | Calcule et transmet l’heure cible de 3 h. |
| `pool=POOL` | Demande le slot du pool partagé avant de lancer la chaîne. |
| `wait_for_completion=True` | Attend que le DAG enfant soit terminé. |
| `deferrable=True` | Confie l’attente au Triggerer et libère le worker. |
| `poke_interval=30` | Définit l’intervalle de vérification de la fin du DAG enfant. |

Les heures de 2 h et 3 h sont calculées comme celle de 1 h,
en remplaçant simplement la valeur du paramètre `hour`.

## 9. Déclenchement de la chaîne 2

`lancer_2 = TriggerDagRunOperator(...)` suit le même principe.

Les différences sont :
- `task_id="lancer_chaine_2"` ;
- `trigger_dag_id="chaine2"` ;
- aucun horaire à transmettre dans `conf`.

Il utilise le même pool et attend également la fin du DAG enfant.

## 10. Dépendances du pilotage

| Ligne | Explication |
|---|---|
| `attendre_1h >> lancer_1` | Autorise le déclenchement de la chaîne 1 après 1 h. |
| `attendre_fichier >> lancer_2` | Autorise le déclenchement de la chaîne 2 après détection d’un fichier. |

Ces deux branches sont indépendantes :
- le fichier peut être détecté pendant que la chaîne 1 tourne ;
- le traitement D doit néanmoins attendre que le pool soit libre.

Si les deux déclencheurs sont prêts en même temps, ce code ne définit pas de priorité métier entre eux.

## 11. Enregistrement des DAGs

| Ligne | Explication |
|---|---|
| `chaine_1()` | Instancie le DAG `chaine1`. |
| `chaine_2()` | Instancie le DAG `chaine2`. |
| `pilotage_chaines()` | Instancie le DAG `pilotage`. |

Ces appels construisent les DAGs pour Airflow.
Ils ne lancent pas immédiatement les scripts Bash.

## 12. Configuration obligatoire

1. Installer le provider Standard dans une version compatible avec Airflow 3.0.
2. Créer la connexion `fs_default`, de type **File (path)**, avec l’extra `{"path": "/"}`.
3. Créer le pool `chaines_exclusives` avec **1 slot**.
4. Activer **Include deferred tasks** sur ce pool.
5. Faire fonctionner le **Triggerer**.
6. Activer les trois DAGs.
7. Lancer les chaînes uniquement via `pilotage`.

Ne pas affecter les jobs Bash au pool partagé :
le déclencheur en occupe déjà l’unique slot et attend leur fin.

Ne pas réinitialiser ou arrêter un déclencheur pendant que sa chaîne tourne :
le DAG enfant pourrait continuer sans que le verrou soit conservé.

## 13. Limites et précautions

- Les scripts doivent retourner 0 en cas de succès et un autre code en cas d’échec.
- Ils doivent attendre la fin réelle du traitement, sans lancement en arrière-plan.
- `job_d.sh` doit revérifier les fichiers et les archiver après succès.
- Le sensor détecte un fichier, mais ne transmet pas son nom à D :
  c’est le script D qui recherche les fichiers à traiter.
- D est lancé au maximum une fois par exécution quotidienne du pilotage.
- Si le pilotage reste actif plus d’une journée, il retarde l’exécution suivante.
- Les heures de 1 h, 2 h et 3 h sont des minimums, pas des garanties de démarrage exact.