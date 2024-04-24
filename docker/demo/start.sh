#!/bin/bash


redis-server /app/redis1.conf
redis-server /app/redis2.conf
/app/redisGunYu -conf /app/gunyu.yaml
