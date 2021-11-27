# Streaming Twitter Sentiment Analyzer

This project represents group work on a UC Berkeley Master of Information and Data Science (MIDS) Final Project for the class "W251 - Scaling Up! Really Big Data," completed August 2018.

Group members: Paul Durkin, Ye (Kevin) Pang, Laura Williams, Walt Burge, Matt Proetsch

In this project, we designed and deployed an infrastructure pipeline for real-time Twitter sentiment analysis, using Linux, IBM Cloud, Kafka, Spark Streaming,  Cassandra, StanfordCoreNLP and Tableau.

## What's in this repository

The final paper and presentation are stored in the main directory of this repo.

Directory contents:

`Ansible` contains configuration playbooks and scripts for managing Kafka, Zookeeper, Spark, and Cassandra cluster nodes.

`Sandbox` contains interim scripts not used in the final solution, including experiments to containerize the process using Docker Swarm, experiments with provisioning via Terraform, and interim streaming scripts.

`SoftlayerTemplates` contains default server configuration options for provisioning machines via Softlayer.

`Utilities` contains scripts for managing and inspecting components of the Cassandra database.

`documentation` contains instructions for multiple components of this project, including server configuration, user interface setup, NLP server setup, and initiating the Kafka/Spark stream.

`monitor` contains scripts for monitoring the health of servers and services throughout the project architecture.

`streaming` contains scripts for initiating the Twitter stream via Kafka and initiating stream processing via Spark.



