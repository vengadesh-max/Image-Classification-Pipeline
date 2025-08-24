# Image Classification ETL Pipeline with Airflow

This project implements an ETL (Extract, Transform, Load) pipeline for image classification using Apache Airflow. The pipeline classifies images based on their resolution (240p, 480p, 720p, 1080p, 4K) using dynamic task mapping.

## Project Structure

```
.
├── docker-compose.yaml          # Docker Compose configuration
├── Dockerfile                   # Custom Airflow image with ML dependencies
├── dags/
│   └── image_classification_dag.py  # Main ETL DAG
├── tasks/
│   └── classify.py              # Image classification logic
├── data/
│   ├── input/                   # Place your images here
│   └── output/                  # Results will be saved here
├── logs/                        # Airflow logs
├── config/                      # Airflow configuration
└── plugins/                     # Airflow plugins
```

## Features

- **3-Task ETL Pipeline**: Extract, Transform, Load with dynamic task mapping
- **Image Resolution Classification**: Automatically categorizes images by resolution
- **Parallel Processing**: Uses Airflow's dynamic task mapping for concurrent image processing
- **Comprehensive Statistics**: Generates detailed analytics and reports
- **Docker-based**: Complete containerized solution

## Prerequisites

- Docker
- Docker Compose
- At least 4GB RAM and 2 CPUs (as recommended by Airflow)

## Quick Start

### 1. Prepare Your Images

Place the images you want to classify in the `data/input/` folder:

```bash
mkdir -p data/input data/output
# Copy your images to data/input/
```

### 2. Build and Start the Services

```bash
# Build the custom Airflow image
docker-compose build

# Start all services
docker-compose up -d
```

### 3. Access Airflow Web UI

- **URL**: http://localhost:8080
- **Username**: airflow
- **Password**: airflow

### 4. Run the Pipeline

1. Go to the Airflow web UI
2. Find the `image_classification_etl` DAG
3. Click "Trigger DAG" to run the pipeline
4. Monitor the execution in the Airflow UI

## Pipeline Overview

### Task 1: Extract (`extract_images`)

- Scans the input folder for image files
- Supports: JPG, JPEG, PNG, BMP, TIFF, WEBP
- Prepares image metadata for processing

### Task 2: Transform (`transform_classify_image`)

- **Dynamic Task Mapping**: Creates parallel tasks for each image
- Classifies each image based on resolution:
  - 240p: ≤ 76,800 pixels (320×240)
  - 480p: ≤ 409,920 pixels (854×480)
  - 720p: ≤ 921,600 pixels (1280×720)
  - 1080p: ≤ 2,073,600 pixels (1920×1080)
  - 4K: > 2,073,600 pixels

### Task 3: Load (`load_aggregate_results`)

- Aggregates all classification results
- Generates comprehensive statistics
- Saves results in JSON and CSV formats

## Output Files

The pipeline generates the following output files in `data/output/`:

- `classification_results_YYYYMMDD_HHMMSS.json`: Complete results with statistics
- `classification_results_YYYYMMDD_HHMMSS.csv`: Tabular data for analysis

## Configuration

### Environment Variables

You can customize the pipeline by setting these environment variables:

```bash
# Input/Output folders
INPUT_FOLDER=/opt/airflow/data/input
OUTPUT_FOLDER=/opt/airflow/data/output

# Airflow settings
AIRFLOW_UID=50000
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow
```

### DAG Configuration

The DAG runs daily by default. You can modify the schedule in `dags/image_classification_dag.py`:

```python
schedule_interval='@daily'  # Change to your preferred schedule
```

## Monitoring and Logs

### View Logs

```bash
# View Airflow logs
docker-compose logs airflow-scheduler
docker-compose logs airflow-worker

# View specific task logs in Airflow UI
```

### Health Checks

All services include health checks. Monitor service status:

```bash
docker-compose ps
```

## Troubleshooting

### Common Issues

1. **Permission Errors**: Ensure proper file permissions

   ```bash
   sudo chown -R 50000:0 ./data
   ```

2. **Memory Issues**: Increase Docker memory allocation to at least 4GB

3. **Port Conflicts**: Change ports in docker-compose.yaml if needed

4. **Image Processing Errors**: Check if images are corrupted or in unsupported formats

### Debug Mode

Run with debug profile for additional tools:

```bash
docker-compose --profile debug up -d
```

## Development

### Adding New Image Formats

Edit `tasks/classify.py` to add support for new image formats:

```python
image_extensions = {'.jpg', '.jpeg', '.png', '.bmp', '.tiff', '.webp', '.new_format'}
```

### Modifying Classification Logic

Update the `classify_resolution` method in `ImageClassifier` class to change classification criteria.

### Custom DAG Parameters

Trigger the DAG with custom parameters:

```python
{
    "input_folder": "/custom/input/path",
    "output_folder": "/custom/output/path"
}
```

## Performance Optimization

- **Parallel Processing**: The pipeline uses dynamic task mapping for concurrent image processing
- **Resource Allocation**: Adjust CPU/memory limits in docker-compose.yaml
- **Batch Processing**: Process images in batches for large datasets

## Security Considerations

- Change default Airflow credentials in production
- Use secrets management for sensitive data
- Implement proper network security
- Regular security updates for base images

## License

This project is licensed under the Apache License 2.0.

## Support

For issues and questions:

1. Check the Airflow logs
2. Review the troubleshooting section
3. Ensure all prerequisites are met
4. Verify image formats are supported
