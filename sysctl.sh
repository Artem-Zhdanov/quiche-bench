#!/bin/bash
sudo sysctl -w net.core.rmem_max=8388608
sudo sysctl -w net.core.rmem_default=8388608
sudo sysctl -w net.ipv4.udp_rmem_min=16384