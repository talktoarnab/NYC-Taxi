# Deployment guides

This document walks through three ways to run the **ETL** (`python -m nyc_taxi`) and the **Streamlit** app (`streamlit run app.py`).

- [1. Streamlit Community Cloud + ETL in GitHub Actions](#1-streamlit-community-cloud--etl-in-github-actions)
- [2. Linux VM (Oracle Cloud Infrastructure)](#2-linux-vm-oracle-cloud-infrastructure)
- [3. Amazon Web Services (AWS)](#3-amazon-web-services-aws)

**Important:** The app reads `output/gold/nyc_taxi_gold.parquet` and `output/kpi/*` on disk. **Streamlit Community Cloud** only gets what is in your **Git** repo (or what you build at runtime). A full month of Gold data is **large**; do not commit it without Git LFS and a clear data policy. The guides below call out the tradeoffs.

---

## 1. Streamlit Community Cloud + ETL in GitHub Actions

### What you are setting up

1. **GitHub Actions** runs the ETL on a schedule (or on demand), validates the pipeline, and stores **build artifacts** (the `output/` folder) on GitHub.
2. **Streamlit Community Cloud** hosts the Streamlit **UI** from the same repository.

**Limitation:** Artifacts from Actions are **not** automatically available to Streamlit’s servers. You must choose one of the following for data on the app:

| Approach | When to use |
|--------|-------------|
| **In-app “Run full ETL pipeline”** | Acceptable for demos; first run is slow and may hit **memory/time limits** on the free Streamlit Cloud tier. |
| **Pre-generated small sample** in repo | Commit only small KPI CSVs (and optionally a **sample** Parquet) for read-only UI; not the full 3M-row file. |
| **Self-host Streamlit** (sections 2–3) + cron ETL | Best for full production and full Gold data. |
| **Object storage (S3) + app changes** | ETL in GHA uploads to S3; app reads from URL/secrets (requires code changes, not in the default `app.py`). |

### Step 1 — Push the project to GitHub

1. Create a new repository (public or private, depending on Streamlit plan).
2. From your machine, in the project root:

   ```bash
   git init
   git add .
   git commit -m "Initial NYC Taxi ETL"
   git remote add origin https://github.com/<your-org>/<repo>.git
   git push -u origin main
   ```

3. Add `data/`, `output/`, and `*.parquet` to **`.gitignore`** if you do not want large downloads committed (typical for this project).

### Step 2 — Add the ETL workflow in GitHub Actions

1. In the repo, create the folder **`.github/workflows/`** (if the repository does not have it in Git yet, add the workflow file from this project: [`.github/workflows/etl.yml`](../.github/workflows/etl.yml)).
2. Commit and push:

   ```bash
   git add .github/workflows/etl.yml
   git commit -m "Add scheduled ETL workflow"
   git push
   ```

3. On GitHub: **Actions** → select **ETL — NYC Taxi** → **Run workflow** (if `workflow_dispatch` is enabled) to test.
4. After a successful run, open the workflow run → **Artifacts** → download `etl-output` to confirm `output/gold/` and `output/kpi/` were produced.

**Cron:** The example workflow can run on a `schedule` (e.g. weekly). Adjust the cron in `etl.yml` to your needs.

### Step 3 — Connect Streamlit Community Cloud

1. Go to [https://streamlit.io/cloud](https://streamlit.io/cloud) and sign in (GitHub account).
2. **New app** → connect your **GitHub** account and select the **repository** and **branch** (usually `main`).
3. **Main file path:** `app.py` (at repository root).
4. **App URL:** Streamlit will assign a URL like `https://<name>.streamlit.app`.

### Step 4 — App settings (optional)

- **Python version:** Set in Streamlit’s **Settings** to match the workflow (3.10+ if available).
- **Secrets:** Not required for public TLC URLs. If you later add S3 or a database, use **Streamlit** → your app → **Settings** → **Secrets** with `toml` key/value pairs.

### Step 5 — How the running app gets data

Pick one path that matches your tolerance for limits and maintenance:

- **A. Rely on the in-app ETL button**  
  A user (or you) opens the app and runs **Run full ETL pipeline**. Ensure the Streamlit app **machine type** has enough **RAM** (full pipeline can use several GB in-process).

- **B. Do not use full Gold on Cloud; ship KPI CSVs only**  
  Add a small optional load path in your fork (e.g. read `output/kpi/*.csv` if present from `git`). The default `app.py` expects the full **Gold** Parquet for the main table; without it, the app **stops** with a warning. For production Cloud, you would either add a “demo mode” or use self-hosting (sections 2–3).

- **C. Use a **fork** that reads Gold from S3** (advanced)  
  After GHA runs ETL, add a job step: `aws s3 sync output/ s3://your-bucket/nyc-taxi/`. Then change `app.py` to load from `st.secrets["GOLD_S3_URI"]` or similar. This is the scalable pattern for Cloud + GHA ETL.

### Checklist (Streamlit + GHA ETL)

- [ ] Workflow runs green on `workflow_dispatch` or `schedule`
- [ ] Artifacts contain expected `output/` layout
- [ ] Streamlit app URL loads
- [ ] You have a documented plan for **where Gold lives** relative to the Cloud app (A, B, or C above)

---

## 2. Linux VM (Oracle Cloud Infrastructure)

This section assumes a **VM.Standard.E2.1** (or similar) **always-free** or paid shape with **Canonical Ubuntu** 22.04 LTS, one **public** IPv4, and a **VNIC** in a public subnet.

### Step 1 — Create the instance (OCI)

1. Sign in to the [Oracle Cloud Console](https://cloud.oracle.com/).
2. **Menu** → **Compute** → **Instances** → **Create instance**.
3. **Image:** Ubuntu 22.04. **Shape:** pick one with **at least 2 OCPUs and 8 GB RAM** for a comfortable full ETL; 1 OCPU / 1 GB may **OOM** on the default pipeline.
4. **Networking:** Create or select a **VCN**; allow a **public** subnet if you will browse to Streamlit on port 8501 (or 80/443 through a reverse proxy).
5. **SSH keys:** Generate or upload your public key; download the private key for login.
6. **Create** and wait until the instance is **Running**; copy the **public IP address**.

### Step 2 — Open the firewall in OCI (Security List + OS)

1. **VCN** → your subnet’s **Security List** → **Ingress rules**:
   - **22** (TCP) from your IP for **SSH** (do not use `0.0.0.0/0` in production for SSH without restrictions).
   - **8501** (TCP) if you will hit Streamlit directly, **or** only **80/443** if you put **Nginx** in front.
2. On the **VM** (first SSH session), if **UFW** is active:

   ```bash
   sudo ufw allow OpenSSH
   sudo ufw allow 8501/tcp
   # or: sudo ufw allow 80,443/tcp
   sudo ufw enable
   ```

### Step 3 — SSH in and install system dependencies

```bash
ssh -i <your-key.pem> ubuntu@<PUBLIC_IP>
```

```bash
sudo apt update && sudo apt install -y python3.10-venv python3-pip git nginx
```

(Adjust Python version to match; 3.10+ is fine.)

### Step 4 — Clone the app and create a virtual environment

```bash
cd /opt
sudo git clone https://github.com/<your-org>/nyc-taxi.git
sudo chown -R $USER:$USER nyc-taxi
cd nyc-taxi
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

### Step 5 — Run the ETL once and verify

```bash
source /opt/nyc-taxi/.venv/bin/activate
cd /opt/nyc-taxi
python -m nyc_taxi
ls -la output/gold/ output/kpi/
```

### Step 6 — Run Streamlit with **systemd** (recommended)

Create a service so the app **restarts** on reboot.

```bash
sudo tee /etc/systemd/system/nyc-taxi-streamlit.service << 'EOF'
[Unit]
Description=NYC Taxi Streamlit
After=network.target

[Service]
User=ubuntu
Group=ubuntu
WorkingDirectory=/opt/nyc-taxi
Environment="PATH=/opt/nyc-taxi/.venv/bin:/usr/bin"
ExecStart=/opt/nyc-taxi/.venv/bin/streamlit run app.py --server.port=8501 --server.address=0.0.0.0 --server.headless=true
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable nyc-taxi-streamlit
sudo systemctl start nyc-taxi-streamlit
sudo systemctl status nyc-taxi-streamlit
```

Test in a browser: `http://<PUBLIC_IP>:8501`.

### Step 7 — Schedule ETL with **cron**

```bash
crontab -e
```

Example: every Sunday at 03:00 (server time):

```cron
0 3 * * 0 /opt/nyc-taxi/.venv/bin/python -m nyc_taxi -q >> /var/log/nyc-taxi-etl.log 2>&1
```

### Step 8 — (Optional) Nginx + HTTPS in front of Streamlit

1. Point a **DNS A record** to the VM’s public IP.
2. Use **Nginx** as a reverse proxy to `127.0.0.1:8501`, or use **Caddy** with automatic HTTPS.
3. Install **Let’s Encrypt** (e.g. `certbot` with the Nginx plugin) for TLS.

**Security:** Harden SSH, use **private** subnet + bastion for production, and restrict **8501** to localhost when using Nginx only on **80/443**.

### Checklist (OCI)

- [ ] Security lists + UFW allow only required ports
- [ ] ETL runs from cron and `output/` is populated
- [ ] `systemctl status nyc-taxi-streamlit` is **active (running)**
- [ ] TLS in front of Streamlit (production)

---

## 3. Amazon Web Services (AWS)

You can deploy the **same** pattern as a Linux VM, or use **containers**. Below: **EC2** (simplest) and a **container** path for **App Runner** / **ECS** / **EC2** with **Docker**.

### Option A — EC2 (Virtual machine, similar to Oracle)

1. **EC2** → **Launch instance**:
   - **AMI:** Ubuntu Server 22.04 LTS.
   - **Instance type:** e.g. **t3.large** (2 vCPU, 8 GiB) or larger for a full ETL run; **t3.small** may **OOM** on the default month slice.
2. **Key pair** for SSH, **Security group**:
   - **22** (your IP) for SSH
   - **8501** for Streamlit (temporary), or **80/443** with Nginx
3. **User data (optional, cloud-init)** to install Git and Python on first boot (or install manually over SSH, same as section 2).
4. **Elastic IP (optional):** allocate and associate so the public IP is stable.
5. **SSH in** and follow **Steps 3–7** in [section 2](#2-linux-vm-oracle-cloud-infrastructure), changing paths to your clone location (e.g. `/opt/nyc-taxi`).

**ETL on a schedule in AWS without managing cron on the box:**

- **EventBridge** rule → **S**SM Run Command, **Lambda** that invokes **S**SM (for stateful runs, prefer **S**S on an EC2 that runs the job), or **AWS Batch** / **Fargate** for containerized ETL. For a single EC2, **cron** on the instance is still the simplest.

### Option B — Docker image (app + optional one-shot ETL)

If this repository includes a **`Dockerfile`**, build and run locally:

```bash
docker build -t nyc-taxi:latest .
```

**Streamlit only** (assumes you mount `output/` with existing data, or you run the ETL in a separate one-shot container first):

```bash
docker run -d -p 8501:8501 -v /home/ubuntu/nyc-taxi-data/output:/opt/nyc-taxi/output nyc-taxi:latest
```

**ETL in a one-off container** (writes to a host volume that the Streamlit container mounts):

```bash
docker run --rm -v /home/ubuntu/nyc-taxi-data:/opt/nyc-taxi nyc-taxi:latest python -m nyc_taxi -q
```

### Option C — AWS App Runner, ECS, or EKS (containers)

1. **Push** the image to **ECR**:
   - `aws ecr create-repository --repository-name nyc-taxi` (if needed)
   - `aws ecr get-login-password | docker login ...`
   - `docker tag` / `docker push`
2. **App Runner** or **ECS Fargate**:
   - **Port:** 8500 or **8501** (match `Dockerfile` / `EXPOSE`).
   - **CPU / memory:** allocate enough **memory** (e.g. 4–8 GiB) if the container runs the **ETL** in the same process as Streamlit; split **ETL** and **web** into two services for a cleaner design.
3. **Load balancer** + **TLS** in front of the service (default on App Runner).

### S3 (optional) for ETL output

- Run ETL in **GHA** or on **EC2** / **Fargate**, then `aws s3 sync output/ s3://<bucket>/nyc-taxi/`.
- The **current** `app.py` does not read S3; you would add loading from `fsspec` / `boto3` and **Streamlit** / **ECS** **secrets** for bucket and prefix. This is the usual “scale” pattern for shared Gold data across many app replicas.

### Checklist (AWS)

- [ ] Security groups lock down SSH; limit **8501** or use Nginx/ALB on **80/443**
- [ ] Instance or task has enough **RAM** for ETL
- [ ] If using Docker, image runs `streamlit` with `--server.address=0.0.0.0` and a stable **port**
- [ ] For production, **separate** ETL (scheduled job) from **read-only** Streamlit replicas, optionally backed by **S3**

---

## Quick reference: ports and processes

| Service | Port | Notes |
|--------|------|--------|
| Streamlit | 8501 (default) | Set `--server.address=0.0.0.0` on servers |
| SSH | 22 | Restrict source IP in cloud security groups |
| Nginx (HTTPS) | 443 | Proxy to `127.0.0.1:8501` |

## Related files in this repository

- [`.github/workflows/etl.yml`](../.github/workflows/etl.yml) — ETL in GitHub Actions
- [`Dockerfile`](../Dockerfile) — optional container build for app + ETL
- [`README.md`](../README.md) — local run and data sources
