#!/bin/bash
sudo sysctl -w net.core.rmem_max=67108864        # 64 MB
sudo sysctl -w net.core.wmem_max=67108864        # 64 MB

# Стандартные значения для новых сокетов
sudo sysctl -w net.core.rmem_default=8388608     # 8 MB
sudo sysctl -w net.core.wmem_default=8388608     # 8 MB

# Минимальные значения UDP-буферов
sudo sysctl -w net.ipv4.udp_rmem_min=262144      # 256 KB
sudo sysctl -w net.ipv4.udp_wmem_min=262144 
