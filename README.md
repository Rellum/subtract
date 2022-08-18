# subtract
PubSub CLI utility

#### Built using [Cobra][cobra]
- `go install github.com/spf13/cobra-cli@latest`
- `go mod init subtract`
- `cobra-cli init --viper`
- `cobra-cli add pull --viper`

[cobra]: [https://github.com/spf13/cobra]


subtract pull \
--subscription=internal-queue-subscription-v1 \
--project=bolcom-pro-sassy-242 \
--max=17400000 >> data/pro-internal-queue-subscription-v1-20220818.log

cat data/pro-internal-queue-subscription-v1-20220818.log | subtract dedup >> data/pro-internal-queue-subscription-v1-20220818-dedup.log

cat data/pro-internal-queue-subscription-v1-20220818-dedup.log | subtract push --project=bolcom-pro-sassy-242 --topic=internal-queue-topic-v1