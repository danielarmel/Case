import subprocess
import os
import sys
# test
def run(command):
    print(f">>> {command}")
    try:
        result = subprocess.run(command, shell=True, check=True,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        print(result.stdout.strip())
    except subprocess.CalledProcessError as e:
        # Combine stderr + stdout and check both for useful output
        error_output = f"{e.stdout.strip()}\n{e.stderr.strip()}".strip()

        print(error_output)

        # Gracefully skip if the resource already exists
        if any(phrase in error_output.lower() for phrase in [
            "already exists",
            "409",
            "duplicate",
        ]):
            print(f"✓ Skipped (already exists): {command}")
        else:
            print(f"✗ Command failed: {command}")
            raise



def main():
    project_id = os.environ.get("PROJECT_ID", f"pyne-dog-breeds")
    region = os.environ.get("REGION", "europe-north1")
    bucket = f"{project_id}-raw"
    sa = f"dog-runner@{project_id}.iam.gserviceaccount.com"

    # Create project
    # run(f"gcloud projects create {project_id}")
    run(f"gcloud config set project {project_id}")

    # Enable APIs
    run("gcloud services enable run.googleapis.com "
        "cloudbuild.googleapis.com "
        "cloudscheduler.googleapis.com "
        "secretmanager.googleapis.com "
        "bigquery.googleapis.com "
        "artifactregistry.googleapis.com "
        "storage.googleapis.com "
        "logging.googleapis.com")

    # Storage + datasets
    run(f"gsutil mb -l {region} gs://{bucket}")
    run(f"bq --location=EU mk --dataset {project_id}:bronze")
    run(f"bq --location=EU mk --dataset {project_id}:silver")
    run(f"bq --location=EU mk --dataset {project_id}:gold")

    # Service account
    run("gcloud iam service-accounts create dog-runner --display-name='Dog Runner'")

    roles = [
        "roles/run.invoker",
        "roles/logging.logWriter",
        "roles/storage.objectCreator",
        "roles/bigquery.dataEditor",
        "roles/bigquery.jobUser",
    ]
    for role in roles:
        run(f"gcloud projects add-iam-policy-binding {project_id} "
            f"--member=serviceAccount:{sa} --role={role}")

    # Artifact Registry
    run(f"gcloud artifacts repositories create pyne-docker "
        f"--repository-format=docker --location={region}")

    print(f"\nBootstrap finished ✅ Project: {project_id}")

if __name__ == "__main__":
    main()
