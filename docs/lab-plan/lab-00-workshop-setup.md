# Lab 00 — Workshop Setup (Orientation + Tools)

**Timebox:** 30–45 minutes  
**Outcome:** You have a clean workspace, the required tools installed, and a “lab notebook” to record outputs.

## What you’re doing in this lab

- Pick a project folder and initialize git
- Install/verify tools you’ll use all day
- Set up a simple log so you can show progress for UAT sign‑off

## You start with

- A computer with internet access
- An AWS account exists (we won’t touch it until Lab 01)

## 00.1 Create a new project folder

Pick a location you’ll remember. Example:

```bash
mkdir -p ~/Developer/fomc-agent-lab
cd ~/Developer/fomc-agent-lab
```

## 00.2 Initialize git (so you can checkpoint your work)

```bash
git init
git status
```

Expected:
- You see “On branch …” and “No commits yet”

## 00.3 Create your lab notebook

Create a simple log file you can paste outputs into during UAT.

```bash
mkdir -p notes
printf "# Workshop Log\n\n" > notes/workshop-log.md
```

Add your name/date at the top:

```bash
printf "Student: <your name>\nDate: <today>\n\n" >> notes/workshop-log.md
```

## 00.4 Install / verify required tools

Run these and record the outputs in `notes/workshop-log.md`.

```bash
git --version
python3 --version
uv --version
aws --version
node --version
```

If something is missing:
- **AWS CLI** (macOS): `brew install awscli`
- **uv** (macOS): `brew install uv`
- **Node** (macOS): `brew install node`

## 00.5 Pick your AWS CLI profile name (write it down)

Pick a dedicated profile name for this workshop and write it at the top of your lab notebook:

- Recommended: `fomc-workshop`

## 00.6 Add a basic `.gitignore` now (save future pain)

Create `.gitignore`:

```bash
cat > .gitignore <<'EOF'
.DS_Store
.env
.env.*
.venv/
__pycache__/
*.pyc
.pytest_cache/
.coverage
dist/
build/
cdk.out/
node_modules/
EOF
```

Checkpoint commit (optional but recommended):

```bash
git add .
git commit -m "Lab 00: workshop setup"
```

## UAT Sign‑Off (Instructor)

- [ ] Project folder exists and is a git repo
- [ ] `notes/workshop-log.md` exists with student name/date
- [ ] Student can run: `python3`, `uv`, `aws`, and `node`
- [ ] `.gitignore` present and includes `.venv/` and `.env*`

Instructor initials: ________  Date/time: ________

## If you finish early (optional extensions)

- Install `jq` and practice pretty-printing JSON: `brew install jq`
- Create a second log file `notes/questions.md` and write down 3 AWS questions you want answered today
