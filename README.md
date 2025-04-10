# Reproducer for issue #130687

## How to execute

 You have for each test case 2 test method:
 - testWithTestContainer: Will start a broker in docker.
 - testWithBroker: Will run the test against an existing broker. Requires additional parameters `-Djcsmp_hostname=tcps://bug-reproducer.messaging.solace.cloud:55443 -Djcsmp_username=admin -Djcsmp_password=password -Dvpn_name=VpnName"`

## Requirements

- java >=17
- maven
- optional: a docker installation to start broker test-container

## Test cases

### First Test

#### Test case
- Create Client A
- Create 3 Flows and provision 3 Queues
- Create producer and send 500msgs that to those queues
- Flow A: receives 10 msgs and blocks its thread
- Flow B: receives 50 msgs and blocks its thread
- Flow C: receives 450 msgs and blocks its thread

#### What I expect:
- Flow A: consumes 10 msgs
- Flow B: consumes 50 msgs
- Flow C: consumes 450 msgs

####  What I see:
- Flow A: consumes 10 msgs
- Flow B: consumes 10 msgs
- Flow C: consumes 10 msgs

Because all flow consumers are running in the same thread: "Context_1_ConsumerDispatcher"

Started via:
`mvn test -Dtest=MultipleFlowsBlockEachOtherTest#testWithBroker -Djcsmp_hostname=tcps://bug-reproducer.messaging.solace.cloud:55443 -Djcsmp_username=admin -Djcsmp_password=password -Dvpn_name=VpnName`

Run from: 2025-04-10 08:57:30,876 +0100
- [log](log/MultipleFlowsBlockEachOtherTest-testWithBroker-001.log)
- [diagnostics](log/gather-diagnostics_3d_nano-sa-production-h2tt9ndk86y-solace-primary-0_2025-04-10T07.49.27.tgz)

### Second Test

#### Test case
- Create Client A
- Create Flow A and provision 1 Queues
- Create producer and send msgs for 5 queues to those queue
- Flow A: consume 100 msgs than block thread
- Stop + close 1 Flow
- Create Flow B


#### What I expect:
- Flow B: consume msgs and receive flow handler message

#### What I see:
- Flow B: is not receiving anything

Because all flow consumers are running in the same thread: "Context_1_ConsumerDispatcher"

Started via:
`mvn test -Dtest=ReproducerTest#testWithBroker -Djcsmp_hostname=tcps://bug-reproducer.messaging.solace.cloud:55443 -Djcsmp_username=admin -Djcsmp_password=password -Dvpn_name=VpnName`

Run from: 2025-04-10 09:23:49,024 +0100
- [log](log/ReproducerTest_testWithBroker-001.log)
- [diagnostics](log/gather-diagnostics_3d_nano-sa-production-h2tt9ndk86y-solace-primary-0_2025-04-10T07.49.27.tgz)
