"""
Image Classification ETL Pipeline DAG

This DAG implements a 3-task ETL pipeline for image classification:
1. Extract: Scan input folder and prepare image list
2. Transform: Classify images based on resolution using dynamic task mapping
3. Load: Aggregate results and save to output location

Author: ML Ops Team
"""

from datetime import datetime, timedelta
import os
import json
import logging
import sys
from typing import List, Dict, Any

# Airflow imports with type ignore
try:
    from airflow import DAG  # type: ignore
    from airflow.operators.python import PythonOperator  # type: ignore
    from airflow.operators.empty import EmptyOperator  # type: ignore
    from airflow.utils.task_group import TaskGroup  # type: ignore
except ImportError:
    # Mock classes for local development
    class DAG:
        def __init__(self, *args, **kwargs):
            pass

    class PythonOperator:
        def __init__(self, *args, **kwargs):
            pass

        @classmethod
        def partial(cls, *args, **kwargs):
            return cls()

        def expand(self, *args, **kwargs):
            return self

    class EmptyOperator:
        def __init__(self, *args, **kwargs):
            pass

    class TaskGroup:
        pass


# Import our custom classification module
sys.path.append("/opt/airflow/tasks")
try:
    from classify import classify_images_task  # type: ignore
except ImportError:
    # Mock function for local development
    def classify_images_task(*args, **kwargs):
        return {"status": "error", "message": "Mock function"}


# Default arguments for the DAG
default_args = {
    "owner": "mlops",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    "image_classification_etl",
    default_args=default_args,
    description="ETL pipeline for image classification based on resolution",
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "image-classification", "mlops"],
    max_active_runs=1,
)


def extract_images(**context) -> List[Dict[str, Any]]:
    """
    Task 1: Extract - Scan input folder and prepare image list for processing

    Returns:
        List of dictionaries containing image metadata
    """
    input_folder = context["dag_run"].conf.get(
        "input_folder", "/opt/airflow/data/input"
    )

    logging.info(f"Scanning input folder: {input_folder}")

    if not os.path.exists(input_folder):
        logging.error(f"Input folder {input_folder} does not exist")
        return []

    # Get all image files
    image_extensions = {".jpg", ".jpeg", ".png", ".bmp", ".tiff", ".webp"}
    image_files = []

    for file in os.listdir(input_folder):
        if any(file.lower().endswith(ext) for ext in image_extensions):
            file_path = os.path.join(input_folder, file)
            file_size = os.path.getsize(file_path)

            image_files.append(
                {
                    "filename": file,
                    "file_path": file_path,
                    "file_size": file_size,
                    "batch_id": context["dag_run"].run_id,
                }
            )

    logging.info(f"Found {len(image_files)} images to process")

    # Store the image list in XCom for the next task
    context["task_instance"].xcom_push(key="image_list", value=image_files)

    return image_files


def transform_classify_image(image_data: Dict[str, Any], **context) -> Dict[str, Any]:
    """
    Task 2: Transform - Classify individual image (used with dynamic task mapping)

    Args:
        image_data: Dictionary containing image metadata

    Returns:
        Dictionary containing classification results
    """
    logging.info(f"Processing image: {image_data['filename']}")

    # Create a temporary folder for single image processing
    temp_folder = f"/tmp/airflow_temp_{context['task_instance'].task_id}"
    os.makedirs(temp_folder, exist_ok=True)

    # Copy the image to temp folder for processing
    import shutil

    temp_image_path = os.path.join(temp_folder, image_data["filename"])
    shutil.copy2(image_data["file_path"], temp_image_path)

    try:
        # Classify the image
        result = classify_images_task(temp_folder)

        # Clean up temp folder
        shutil.rmtree(temp_folder, ignore_errors=True)

        if result["status"] == "success" and result["results"]:
            # Extract the classification result for this specific image
            image_result = result["results"][0]
            image_result["batch_id"] = image_data["batch_id"]
            image_result["original_file_path"] = image_data["file_path"]

            logging.info(
                f"Successfully classified {image_data['filename']}: {image_result['resolution_category']}"
            )
            return image_result
        else:
            logging.error(f"Failed to classify {image_data['filename']}")
            return {
                "filename": image_data["filename"],
                "status": "error",
                "error_message": "Classification failed",
                "batch_id": image_data["batch_id"],
            }

    except Exception as e:
        logging.error(f"Error processing {image_data['filename']}: {str(e)}")
        # Clean up temp folder
        shutil.rmtree(temp_folder, ignore_errors=True)

        return {
            "filename": image_data["filename"],
            "status": "error",
            "error_message": str(e),
            "batch_id": image_data["batch_id"],
        }


def load_aggregate_results(**context) -> Dict[str, Any]:
    """
    Task 3: Load - Aggregate all classification results and save to output

    Returns:
        Dictionary containing aggregated results and statistics
    """
    # Get all results from the previous task
    task_instance = context["task_instance"]

    # Get results from all mapped tasks
    mapped_results = []

    # Get the task instance of the transform task
    transform_task = context["dag"].get_task("transform_classify_image")

    # Collect results from all mapped instances
    for map_index in range(len(context["dag_run"].conf.get("image_list", []))):
        try:
            result = task_instance.xcom_pull(
                task_ids="transform_classify_image",
                dag_id=context["dag"].dag_id,
                run_id=context["dag_run"].run_id,
                key=f"return_value_{map_index}",
            )
            if result:
                mapped_results.append(result)
        except Exception as e:
            logging.warning(f"Could not get result for map_index {map_index}: {e}")

    logging.info(f"Aggregated {len(mapped_results)} classification results")

    # Separate successful and failed classifications
    successful_results = [r for r in mapped_results if r.get("status") != "error"]
    failed_results = [r for r in mapped_results if r.get("status") == "error"]

    # Generate aggregated statistics
    if successful_results:
        import pandas as pd

        df = pd.DataFrame(successful_results)

        # Calculate statistics
        stats = {
            "total_images_processed": len(mapped_results),
            "successful_classifications": len(successful_results),
            "failed_classifications": len(failed_results),
            "resolution_distribution": df["resolution_category"]
            .value_counts()
            .to_dict(),
            "avg_file_size_mb": (
                round(df["file_size_mb"].mean(), 2)
                if "file_size_mb" in df.columns
                else 0
            ),
            "total_size_mb": (
                round(df["file_size_mb"].sum(), 2)
                if "file_size_mb" in df.columns
                else 0
            ),
            "avg_aspect_ratio": (
                round(df["aspect_ratio"].mean(), 2)
                if "aspect_ratio" in df.columns
                else 0
            ),
        }
    else:
        stats = {
            "total_images_processed": len(mapped_results),
            "successful_classifications": 0,
            "failed_classifications": len(failed_results),
            "resolution_distribution": {},
            "avg_file_size_mb": 0,
            "total_size_mb": 0,
            "avg_aspect_ratio": 0,
        }

    # Save results to output location
    output_folder = context["dag_run"].conf.get(
        "output_folder", "/opt/airflow/data/output"
    )
    os.makedirs(output_folder, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Save detailed results as JSON
    results_file = os.path.join(
        output_folder, f"classification_results_{timestamp}.json"
    )
    with open(results_file, "w") as f:
        json.dump(
            {
                "batch_id": context["dag_run"].run_id,
                "timestamp": timestamp,
                "statistics": stats,
                "successful_results": successful_results,
                "failed_results": failed_results,
            },
            f,
            indent=2,
        )

    # Save successful results as CSV
    if successful_results:
        csv_file = os.path.join(
            output_folder, f"classification_results_{timestamp}.csv"
        )
        df.to_csv(csv_file, index=False)
        logging.info(f"Results saved to {csv_file}")

    logging.info(f"Final results saved to {results_file}")
    logging.info(f"Statistics: {stats}")

    return {
        "status": "success",
        "output_files": [results_file],
        "statistics": stats,
        "batch_id": context["dag_run"].run_id,
    }


# Task definitions
start_task = EmptyOperator(task_id="start", dag=dag)

extract_task = PythonOperator(
    task_id="extract_images", python_callable=extract_images, dag=dag
)

# Dynamic task mapping for transform
transform_task = PythonOperator.partial(
    task_id="transform_classify_image",
    python_callable=transform_classify_image,
    dag=dag,
).expand(image_data=extract_task.output)

load_task = PythonOperator(
    task_id="load_aggregate_results", python_callable=load_aggregate_results, dag=dag
)

end_task = EmptyOperator(task_id="end", dag=dag)

# Task dependencies
start_task >> extract_task >> transform_task >> load_task >> end_task
