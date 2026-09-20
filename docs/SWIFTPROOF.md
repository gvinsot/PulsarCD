# SwiftProof dans le pipeline PulsarCD

SwiftProof ajoute une revue vérifiable entre les tests et le déploiement. Son
activation est indépendante du mode de transition (`auto`, `agent`, `manual`).
Le même contrôle est appliqué dans `StackDeployer.deploy`, pour les appels de
l'interface, de l'API, du pipeline et du MCP.

## Activer un projet

1. Publier cette version de PulsarCD et mettre à jour ses scripts sur l'hôte
   de déploiement. Depuis une copie relue de PulsarCD sur cet hôte Linux :
   `sudo bash scripts/install-swiftproof.sh`. L'installateur vérifie le SHA-256
   de la version SwiftProof v0.1.0, pour amd64 ou arm64. Un autre emplacement
   est possible avec un argument de l'installateur, puis la variable
   `PULSARCD_SWIFTPROOF_BINARY` dans le service PulsarCD.
2. Vérifier Python 3, PyYAML, Git, Docker et Buildx sur cet hôte. Utiliser le
   même compte SSH et le même répertoire personnel pour les builds et les
   déploiements. Pour utiliser le LLM via SSH, autoriser le transfert de port
   distant vers `127.0.0.1` ; aucun port public supplémentaire n'est nécessaire.
3. Faire relire et intégrer `.swiftproof.json` dans le projet. Préparer puis
   précharger son image de tests avec toutes les dépendances, idéalement par
   digest. Les tests SwiftProof ne téléchargent pas les dépendances. La politique
   sera lue depuis le commit de production, pas depuis le candidat.
4. Après ce premier déploiement de la politique, relever et vérifier le SHA Git
   complet réellement en production. Dans **Stacks → Test → Deploy**, cocher
   **Require SwiftProof before deployment**, renseigner ce SHA initial et
   enregistrer. Cette initialisation est une déclaration de l'administrateur :
   SwiftProof ne peut pas déduire le commit d'anciennes images sans provenance.
5. Laisser **Use the LLM configured in PulsarCD** coché. Aucune URL, clé ou
   sélection de modèle supplémentaire n'est nécessaire. Reconstruire la prochaine
   version avec les nouveaux scripts pour disposer de sa provenance.

La fonctionnalité est désactivée par défaut sur les projets existants. Le pilote
GitHub de SwiftProof est informatif ; le contrôle PulsarCD, une fois activé,
bloque effectivement les déploiements qui ne satisfont pas la politique.

## Réutilisation du LLM

Chaque revue lit la configuration LLM effective de PulsarCD, y compris ses
surcharges d'environnement. Une passerelle temporaire transmet les appels Chat
Completions au modèle configuré. La clé du fournisseur reste dans le processus
PulsarCD ; SwiftProof reçoit un jeton temporaire par l'entrée standard SSH.
Le candidat ne choisit ni le fournisseur ni le modèle. Les outils MCP et les
droits de déploiement de l'agent PulsarCD ne sont pas exposés à cette revue.

Limites : 20 appels, 4 096 tokens de sortie maximum par appel (ou la limite
PulsarCD si inférieure), 128 Kio par requête, 1 Mio par réponse, 10 tests générés
et 10 minutes pour l'investigation LLM. La politique SwiftProof conserve ses
limites plus basses. La commande complète a une limite de 30 minutes et les
preuves transférées une limite de 8 Mio. Le code transmis au modèle bénéficie
du masquage de SwiftProof, qui reste une protection au mieux.

Décocher l'option LLM conserve les analyses et tests déterministes. Une absence
de modèle/URL désactive l'investigateur. Une panne du fournisseur laisse une
investigation incomplète à examiner ; elle ne produit pas une approbation.
Les transitions PulsarCD en mode `agent` sont désormais refusées si leur agent
LLM est indisponible.

## Décisions et preuves

| Résultat SwiftProof | Déploiement |
| --- | --- |
| 0 | Autorisé par SwiftProof ; les autres contrôles restent applicables |
| 1 | Bloqué : problème élevé/critique reproduit |
| 2 | Revue humaine requise |
| 3 ou 4, rapport absent/invalide | Bloqué : configuration ou exécution à corriger |

Les rapports Markdown sont affichés comme du texte, et l'archive contient le
JSON ainsi que les preuves conservées. Pour un code 2, un administrateur
connecté peut approuver avec un motif, puis relancer explicitement le
déploiement. Il ne peut pas approuver un code 1, 3 ou 4. Le bouton de nouvelle
revue invalide le résultat courant ; la prochaine tentative regénère les
preuves. Les anciennes archives restent disponibles pour l'audit.

L'identifiant du rapport lie le SHA de production, le SHA candidat, les digests
des images, le binaire, la politique de référence et la configuration du modèle.
Tout changement de ces éléments impose une nouvelle revue. Les builds créent
une provenance dans `~/.local/share/pulsarcd/swiftproof/builds/`. Les versions
doivent être exactes (`1.2.3`, tag `v1.2.3`). Réutiliser des images exige leur
provenance existante correspondant au même commit. Sinon, reconstruire **toutes**
les images avec `--no-cache`, plutôt que d'attribuer un ancien binaire à un
nouveau commit. Les projets sans images construites ne sont pas couverts par
ce premier adaptateur de provenance.

Avant le déploiement, PulsarCD revérifie cette identité, sélectionne le SHA
candidat exact et remplace les images du compose par leurs digests. Les images
tierces doivent déjà être épinglées par digest. Les variantes de build restent
distinctes. Après succès, les images présentes dans les spécifications Swarm
sont comparées aux digests attendus puis la nouvelle référence est enregistrée
dans `~/.local/share/pulsarcd/swiftproof/deployed/`. Cela vérifie les images
demandées au Swarm, pas la santé de toutes les répliques. Un déploiement QA ne
modifie jamais la référence de production et sa promotion reste manuelle.

Les rapports PulsarCD sont dans `<data_dir>/swiftproof/`. Sauvegarder ce
répertoire, `pipeline_state.json` et les données de provenance de l'hôte.
La provenance est un enregistrement de confiance du build, pas une attestation
cryptographique indépendante. L'administrateur de l'hôte, Docker, les scripts
PulsarCD et les hooks de déploiement font partie du périmètre de confiance.
Le contrôle ne protège pas contre un administrateur qui modifie directement
ces fichiers ou déploie hors de PulsarCD. Prévoir une seule instance PulsarCD
active pour sérialiser les déploiements d'un même projet.

Les détails des interfaces utilisées sont documentés par
[Docker Buildx](https://docs.docker.com/reference/cli/docker/buildx/imagetools/inspect/)
et [AsyncSSH](https://asyncssh.readthedocs.io/en/latest/api.html#asyncssh.SSHClientConnection.forward_remote_port).

## Vérification locale

```sh
python -m pytest tests/test_swiftproof.py tests/test_api.py tests/test_security.py tests/test_build_push.py -q
SWIFTPROOF_TEST_BINARY=/chemin/swiftproof python -m pytest tests/test_swiftproof.py -q
```

Le second appel ajoute le vrai binaire au test de bout en bout avec un
fournisseur local simulé. Les tests ne déploient pas sur le cluster et ne
contactent pas le LLM de production.
