FROM ubuntu:22.04

RUN apt-get update && apt-get install -y openssh-server openjdk-17-jdk wget nano sudo iputils-ping

RUN useradd -m -s /bin/bash hadoop && echo "hadoop:hadoop" | chpasswd && adduser hadoop sudo

USER hadoop
WORKDIR /home/hadoop

RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 0600 ~/.ssh/authorized_keys

RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-3.5.0/hadoop-3.5.0.tar.gz && \
    tar -xzf hadoop-3.5.0.tar.gz && mv hadoop-3.5.0 hadoop

RUN echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64' >> ~/.bashrc && \
    echo 'export HADOOP_HOME=/home/hadoop/hadoop' >> ~/.bashrc && \
    echo 'export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin' >> ~/.bashrc

EXPOSE 9870 8088 9000
CMD ["sudo", "/usr/sbin/sshd", "-D"]