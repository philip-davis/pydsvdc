#!/bin/bash

unset VDC_CREDENTIALS
unset VDC_SOCKET

echo "Enter VDC username:"
read uname
echo "Enter VDC password:"
read -s passwd
export VDC_CREDENTIALS="${uname}:${passwd}"
export VDC_SOCKET=155.98.19.235:27017
echo "VDC credentials saved for this session"
