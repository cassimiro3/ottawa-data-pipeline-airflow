# 🌊 ottawa-data-pipeline-airflow - Build Your Own Modern Data Lake

[![Download](https://img.shields.io/badge/Download%20now-ottawa--data--pipeline--airflow-blue?style=flat-square)](https://github.com/cassimiro3/ottawa-data-pipeline-airflow/releases)

## 🚀 Getting Started

Welcome! This guide will help you to download and run the ottawa-data-pipeline-airflow project. You will set up a complete data engineering project using tools like Apache Airflow, LocalStack S3, MySQL, MongoDB, and Elasticsearch.

### 🖥️ System Requirements

Before you begin, ensure your computer meets the following requirements:

- Operating System: Windows, macOS, or Linux
- Docker: Install Docker Desktop or Docker Engine.
- RAM: At least 8GB
- Storage: At least 10GB of free disk space
- Internet Connection: Required for initial setup and downloads

## 📥 Download & Install

To get started, visit the [Releases page](https://github.com/cassimiro3/ottawa-data-pipeline-airflow/releases) to download the latest version of the software. 

1. Click on the link above.
2. Find the latest release.
3. Download the appropriate file for your operating system.

## 🛠️ How to Run

Once you have downloaded the files, follow these steps to run the application:

1. **Extract Files**: Locate the downloaded file and extract it to a folder on your computer.
2. **Open Terminal or Command Prompt**:
   - For Windows: Open Command Prompt.
   - For macOS/Linux: Open Terminal.
3. **Navigate to the Folder**: Change to the directory where you extracted the files. Use the command:
   ```bash
   cd path/to/extracted/folder
   ```
4. **Start the Application**: Run the following command to start the Docker containers:
   ```bash
   docker-compose up
   ```

5. **Access the Web Interface**: Open your web browser and go to `http://localhost:8080` to access Apache Airflow's web interface.

## 🌐 Features

This project provides several features that enhance your data engineering workflow:

- **Data Lake Architecture**: Simulates a complete data lake, moving data from Raw to Staging, then to Curated and finally Indexed formats.
- **Task Scheduling**: Uses Apache Airflow to schedule and monitor workflows efficiently.
- **Data Storage**: Supports multiple databases, including MySQL and MongoDB for flexible data storage options.
- **LocalStack S3**: Provides a local environment for testing with S3 storage.
- **Search & Analyze**: Incorporates Elasticsearch, allowing you to search and analyze data effectively.

## ⚙️ Configuration 

You may want to customize some settings before running the software:

- **Database Configuration**: Modify the `docker-compose.yml` file to set up your database connection details according to your preference.
- **Airflow Variables**: You can add variables within the Airflow UI to meet specific workflow needs.

## 💡 Tips

- Ensure Docker is running before executing `docker-compose up`.
- If you encounter issues, check that you’ve allocated sufficient resources to Docker via settings.

## 📃 Documentation

For more detailed usage and advanced configurations, refer to the official Apache Airflow documentation [here](https://airflow.apache.org/docs/).

## 💬 Support

If you experience any problems or have questions, you can open an issue on the GitHub repository. The community is ready to help!

Don't forget to check out the [Releases page](https://github.com/cassimiro3/ottawa-data-pipeline-airflow/releases) for the latest updates! 

Happy data engineering!