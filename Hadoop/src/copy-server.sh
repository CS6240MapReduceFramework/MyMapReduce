
secGroup="launch-wizard-1"
keyPair="kaushikfinaaws"

insIp="52.91.229.45"

scp -i $keyPair.pem server.jar ec2-user@$insIp:~
scp -i $keyPair.pem $keyPair.pem ec2-user@$insIp:~


ssh -i $keyPair.pem ec2-user@$insIp "java -jar server.jar > log.txt" &