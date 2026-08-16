# Publish this GitHub profile README

GitHub displays a profile README only when a public repository has exactly the
same name as the account.

1. Sign in to `Dipti-Coder202`.
2. Create a new **public** repository named `Dipti-Coder202`.
3. Do not initialize it with another README.
4. From the `DataEngineering` repository root, run the commands below after
   replacing `<PROFILE_REPOSITORY_URL>` with the URL GitHub shows:

```bash
cd Dipti-Coder202_Profile
git init -b main
git add -- README.md
git commit -m "Add professional Data Engineer profile README"
git remote add origin <PROFILE_REPOSITORY_URL>
git push -u origin main
```

After pushing, open <https://github.com/Dipti-Coder202> and confirm the README
appears on the profile overview.

Recommended GitHub profile settings:

- Name: `Akash Giri`
- Bio: `Data Engineer | 3 years | Python, SQL, PySpark, Airflow, Kafka & PostgreSQL`
- Location: add your real city and country
- Website: add LinkedIn or a portfolio only if it is current
- Available for hire: enable when appropriate

Pin the most relevant repositories after publishing the README. Start with
`DataEngineering`; add separate project repositories later if you split the
monorepo into focused portfolios.
