#!bin/bash
cd /sys/class/net/eth0/statistics
end=$((SECONDS+10));
old_rx="$(</sys/class/net/eth0/statistics/rx_bytes)";
old_tx="$(</sys/class/net/eth0/statistics/tx_bytes)";
while $(sleep 1); do
        if [ $SECONDS -lt $end ]; then
                now_rx=$(<rx_bytes);
                now_tx=$(<tx_bytes);
                echo 1,$((($now_rx-$old_rx))),$((($now_tx-$old_tx)));
                old_rx=$now_rx;
                old_tx=$now_tx;
        else
                break
        fi
done