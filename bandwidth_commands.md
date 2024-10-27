# Monitoring the bandwith

Install the pv package using Yum. Monitor the packets with `tcpdump` and count the bytes with `pv`.

```
sudo yum install pv
sudo tcpdump -ni ens33 udp -w- |pv -i2 >/dev/null
```

Map for IP and Node
```
VM01 - 172.22.156.212
VM02 - 172.22.158.212
VM03 - 172.22.94.212
VM04 - 172.22.156.213
VM05 - 172.22.158.213
VM06 - 172.22.94.213
VM07 - 172.22.156.214
VM08 - 172.22.158.214
VM09 - 172.22.94.214
VM10 - 172.22.156.215
```