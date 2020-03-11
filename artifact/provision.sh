#!/bin/bash -eux

wget -O- https://deb.nodesource.com/setup_8.x | bash -


# For Xenial we need:
#  - install R package server
apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E084DAB9
echo "deb https://cran.rstudio.com/bin/linux/ubuntu xenial-cran35/" > /etc/apt/sources.list.d/r-lang.list


apt-get update
apt-get install -y --allow-unauthenticated r-base
apt-get install -y openjdk-8-jdk openjdk-8-source python-pip ant nodejs

pip install git+https://github.com/smarr/ReBench

# install Latex
apt-get --no-install-recommends install -y texlive-base texlive-latex-base texlive-fonts-recommended  texlive-latex-extra texlive-fonts-extra cm-super


# enable nice without sudo
echo "artifact       -     nice       -20" >> /etc/security/limits.conf


su artifact <<SHELL
# Create .Rprofile for installing libraries
echo "options(repos=structure(c(CRAN=\"https://cloud.r-project.org/\")))" > .Rprofile
SHELL

# install R dependencies
pushd evaluation/scripts
Rscript libraries.R
popd