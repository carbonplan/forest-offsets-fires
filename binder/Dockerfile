# Inherit from an upstream image
FROM jupyter/base-notebook:2023-05-15

COPY binder/environment.yml /tmp/environment.yml

RUN mamba config --env --add channels conda-forge
RUN mamba config --env --set channel_priority strict
RUN mamba env update --prefix ${CONDA_DIR} --file /tmp/environment.yml

COPY binder/image-tests image-tests
RUN ls
