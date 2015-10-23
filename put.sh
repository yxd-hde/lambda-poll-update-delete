#!/bin/bash

function print20lines {
    for i in 1 .. 20; do
        echo $i
    done
}

print20lines | parallel -N0 -j20 ./put_reqs_to_sqs.py
