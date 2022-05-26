FROM ubuntu:22.04

# ROOT deps
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive TZ=Europe/Zurich apt-get -y -q install \
    dpkg-dev cmake g++ gcc binutils libx11-dev libxpm-dev \
    libxft-dev libxext-dev libssl-dev gfortran libpcre3-dev \
    xlibmesa-glu-dev libglew-dev libftgl-dev \
    libmysqlclient-dev libfftw3-dev libcfitsio-dev \
    graphviz-dev libavahi-compat-libdnssd-dev \
    libldap2-dev libxml2-dev libkrb5-dev \
    libgsl0-dev git python3 python3-pip

# Python deps
RUN pip install numpy jupyter

# Build ROOT
RUN git clone https://github.com/root-project/root.git root_src
RUN mkdir root_build
RUN cmake -Dgminimal=ON -Dasimage=ON -Ddev=ON \
          -Druntime_cxxmodules=ON -Dgnuinstall=ON -Dsonames=ON \
          -Dbuiltin_xrootd=ON -Dxrootd=ON -Dbuiltin_davix=ON -Ddavix=ON \
          -Dpyroot=ON -Ddataframe=ON \
          -B root_build -S root_src
RUN cmake --build root_build -- install -j$(nproc)

# Remove artifacts
RUN rm -rf root_build root_src

# Change environment so that libraries are properly found
RUN echo /usr/local/lib/root >> /etc/ld.so.conf && ldconfig
ENV PYTHONPATH /usr/local/lib/root:$PYTHONPATH
ENV CLING_STANDARD_PCH none

# Start a jupyter notebook
EXPOSE 8888
CMD ["rootnb.exe", "--port=8888", "--no-browser", "--allow-root", "--ip=0.0.0.0"]
