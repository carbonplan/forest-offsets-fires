# Inherit from an upstream image
FROM pangeo/base-image:2023.05.08

COPY environment.yml /tmp/environment.yml

RUN mamba env update --prefix ${CONDA_DIR} --file /tmp/environment.yml

COPY image-tests image-tests
RUN ls