# Former Repository History

These bundles retain the complete committed histories of the two Forgejo seed
repositories that were formerly nested Git worktrees:

- `gateway-c37-118.bundle`
- `processor-alarm-threshold.bundle`

The current seed source lives in the sibling directories and is what
`forgejo-init` uses during bootstrap. The bundles are not mounted into the
bootstrap container. To inspect or recover a standalone history, clone its
bundle, for example:

```sh
git clone gateway-c37-118.bundle gateway-c37-118
```