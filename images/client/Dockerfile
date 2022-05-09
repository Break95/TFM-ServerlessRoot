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

# ROOT
RUN git clone https://github.com/root-project/root.git root_src
RUN mkdir root_build
RUN cmake -Dminimal=ON -Dpyroot=ON -Ddataframe=ON -Dbuiltin_xrootd=ON -Dxrootd=ON -Dbuiltin_davix=ON -Ddavix=ON -Dgnuinstall=ON -DCMAKE_INSTALL_PREFIX=root_install -B root_build -S root_src
RUN cmake --build root_build -- install -j$(nproc)

RUN rm -rf root_build root_src

ENV PYTHONPATH /root_install/lib/root

CMD  /bin/bash -c "source /root_install/bin/thisroot.sh && jupyter notebook --allow-root"
