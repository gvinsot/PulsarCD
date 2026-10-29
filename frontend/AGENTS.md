# Interface sans compilation

- Les fichiers JS sont des scripts classiques partageant un contexte global,
  chargés dans l'ordre de `index.html`. Vérifier cet ordre et les noms globaux
  avant d'ajouter ou de déplacer une fonction.
- `static/js/core/` contient les fonctions communes ; `static/js/views/` les
  vues ; `app.js` contient notamment le registre `UI_HANDLERS`.
- Utiliser les attributs `data-click`, `data-change`, etc. et `uiArgs()` pour
  les événements ; enregistrer les nouveaux handlers dans `UI_HANDLERS`.
  Respecter la CSP (`core/csp.js`), éviter les handlers inline et échapper les
  données affichées avec les helpers existants dans `core/utils.js`.
- Les feuilles CSS sont séparées par domaine et ordonnées dans `index.html`.
  Lire [FRONTEND_PERFORMANCE.md](../docs/FRONTEND_PERFORMANCE.md) pour les
  animations, le rendu et les vues mises à jour régulièrement.
- Nettoyer les timers, abonnements et ressources d'une vue lorsqu'elle se ferme.
- Tests : `bash scripts/dev.sh frontend` ou
  `bash scripts/dev.sh frontend tests/test_stack_env.cjs`. Ils utilisent les
  modules natifs de Node et des doubles DOM, sans installation npm.
- Les tests Node ne remplacent pas une vérification visuelle des changements
  de présentation ; indiquer explicitement si elle n'a pas été effectuée.
