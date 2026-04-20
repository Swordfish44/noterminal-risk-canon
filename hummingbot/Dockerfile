FROM hummingbot/hummingbot:latest

# Install psycopg2-binary into the hummingbot conda environment
RUN /opt/conda/envs/hummingbot/bin/pip install psycopg2-binary --quiet

# Ensure required conf subdirectories exist at image build time
RUN mkdir -p /home/hummingbot/conf/connectors /home/hummingbot/conf/strategies
