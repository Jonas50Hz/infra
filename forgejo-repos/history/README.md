# Former repository history

These bundles retain the complete committed histories of Forgejo seed
repositories that were formerly nested Git worktrees. They are archives only:
they are not mounted into the bootstrap container and `forgejo-init` does not
read them. The live seed source is the sibling directory tree.

| Bundle | Lineage | Head |
| --- | --- | --- |
| `gateway-c37-118-current.bundle` | The current gateway lineage, de-nested into this repository. Begins at `3ea3270 Rebuild C37.118 gateway`. | `5c75129 Support catalog-driven gateway recovery` |
| `gateway-c37-118-prerebuild.bundle` | The **pre-rebuild** gateway lineage, a separate implementation that was replaced by the rebuild. It has no commit in common with the current lineage. | `76ba93f` on `fix/c37-v2-quality-aware-verifier` |
| `processor-alarm-threshold.bundle` | The alarm-threshold processor history from when it was a nested worktree. | `f611ea7` on `feature/add-alarm-threshold-processor` |

The two gateway bundles are **different implementations, not old and new
revisions of one line**. The pre-rebuild lineage owns
`config.py`, `publisher.py`, `reconciliation.py`, and
`scripts/reconcile_masterdata.py`; the current lineage owns `catalog.py`,
`masterdata_publisher.py`, and `scripts/deploy_gateway.py`. Neither history
reaches the other, so the pre-rebuild bundle is the only surviving copy of that
earlier design. Do not delete it.

To inspect or recover a standalone history, clone its bundle:

```sh
git clone forgejo-repos/history/gateway-c37-118-prerebuild.bundle gateway-prerebuild
```

To list what a bundle contains without cloning:

```sh
git bundle list-heads forgejo-repos/history/gateway-c37-118-prerebuild.bundle
```
