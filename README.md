# Varath Builds It

Working code from the **Varath Builds It** series on
[Data & AI with Varath](https://www.youtube.com/@DataAndAIwithVarath).

Every episode builds one real data-engineering system, step by step.
Each folder below is a complete, self-contained project: fork the repo,
open the folder from the episode you watched, and follow its README.

| # | Build | Stack | Video |
|---|---|---|---|
| 01 | [Metadata-driven REST API ingestion](01-fabric-api-ingestion/) | Microsoft Fabric · PySpark · Data Factory · Power BI Direct Lake | *link after upload* |

## How the builds are organised

```
varath-builds/
├── 01-fabric-api-ingestion/     ← one folder per episode
│   ├── README.md                ← setup steps + design decisions
│   ├── config/  notebooks/  sql/  pipeline/
├── 02-.../                      ← next episode
└── LICENSE                      ← MIT, covers every build
```

Only need one build? Use **Code → Download ZIP**, or fetch a single folder:

```bash
git clone --filter=blob:none --sparse https://github.com/vrajans/varath-builds.git
cd varath-builds
git sparse-checkout set 01-fabric-api-ingestion
```

## Suggest the next build

Open an issue or comment on the latest video with the system you want to see
built. The most-requested ideas go to the top of the list.

MIT licence: use it, fork it, ship it.
