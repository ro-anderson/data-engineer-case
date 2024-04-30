# Base image
FROM python:3.10 AS base

# Set the working directory in the container
WORKDIR /app

# Install unzip and curl (if needed for poetry installation)
RUN apt-get update && apt-get install -y unzip curl

# Install poetry
RUN curl -sSL https://install.python-poetry.org | python -

# Update the PATH to include the Poetry binary
ENV PATH="/root/.local/bin:${PATH}"

# Copy the local directory contents into the container
COPY . /app

# Install the required dependencies using Poetry
RUN poetry install

# Expose the port the app runs on
EXPOSE 8888
