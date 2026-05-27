# Releasing a V2 Patch

How to ship a 2.x patch while `main` is on 3.x. The 2.x line lives on a
long-lived branch, **`V2-Main`**, started from the last V2 release, `2.3.1`.

## Procedure

**For each fix:**
1. Branch `V2-<feature>` off `V2-Main`.
2. Open a PR `V2-<feature>` → `V2-Main`. Tests run automatically as a PR check.
3Merge the PR. The merge auto-publishes `<jarVersion>-SNAPSHOT` (e.g. `2.3.2-SNAPSHOT`).

**To release:**
1. GitHub → **Actions** → **Build Release Branch** → Run workflow → `branch = V2-Main`.
   Publishes the release version `<jarVersion>` (e.g. `2.3.2`) to Sonatype.
2. Bump `jarVersion` on `V2-Main` to the next patch for ongoing snapshots.
3. *(Optional)* tag the released commit: `git tag 2.3.2 <sha> && git push origin 2.3.2`.

`main` / 3.x is never touched.

---

## How the version is set

The published version comes from `build.gradle`, **not** the git tag:

```gradle
def jarVersion = "2.3.2"
def isRelease = System.getenv("BUILD_EVENT") == "release"
version = isRelease ? jarVersion : jarVersion + "-SNAPSHOT"
```

- Whatever a workflow builds, it publishes the `jarVersion` from *that* commit.
- It's a `-SNAPSHOT` unless `BUILD_EVENT == "release"` (only the release workflow sets that).
- This project does **not** read `BRANCH_REF_NAME`, so the branch name is never
  appended — you get `2.3.2-SNAPSHOT`, not `2.3.2-V2-Main-SNAPSHOT`.

## The workflows

Each trigger reads its workflow file from a different place, so each file has to
live on a specific branch:

| Workflow                         | File                        | Trigger                      | Lives on  | Result                             |
|----------------------------------|-----------------------------|------------------------------|-----------|------------------------------------|
| Build Pull Request               | `build-pr.yml`              | `pull_request` (any base)    | `V2-Main` | tests run as a PR check            |
| Build a Branch Specific Snapshot | `build-branch-snapshot.yml` | `push` to `V2-Main`          | `V2-Main` | publishes `<jarVersion>-SNAPSHOT`  |
| Build Release Branch             | `build-release-branch.yml`  | `workflow_dispatch` (manual) | `main`    | publishes `<jarVersion>` (release) |

The "lives on" column is not a preference — it's a GitHub rule:

- **`push`** runs the workflow from the *pushed* branch → snapshot must be on `V2-Main`.
- **`pull_request`** runs the workflow from the PR's *base* branch → PR build must be on `V2-Main`.
- **`workflow_dispatch`** only shows its "Run workflow" button from the *default*
  branch → the manual release tool must be on `main`. It checks out the branch
  you type, so it can release `V2-Main` from `main`.
