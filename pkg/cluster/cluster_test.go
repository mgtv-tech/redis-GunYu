package cluster

import (
	"context"
	"testing"

	"github.com/ikenchina/redis-GunYu/config"

	"github.com/stretchr/testify/suite"
)

func TestClusterSuite(t *testing.T) {
	suite.Run(t, new(clusterTestSuite))
}

type clusterTestSuite struct {
	suite.Suite

	cliA *Cluster
	cliB *Cluster
	valA string
	valB string

	ttl int

	pref1 string
}

func (ts *clusterTestSuite) SetupTest() {
	ts.pref1 = "/test/election"
	ts.ttl = 3
	var err error
	ts.cliA, err = NewCluster(context.Background(), config.EtcdConfig{Endpoints: []string{"localhost:2379"}, Ttl: ts.ttl})
	ts.Nil(err)
	ts.valA = "A"

	ts.cliB, err = NewCluster(context.Background(), config.EtcdConfig{Endpoints: []string{"localhost:2379"}, Ttl: ts.ttl})
	ts.Nil(err)
	ts.valB = "B"
}

func (ts *clusterTestSuite) TearDownTest() {
	ts.cliA.Close()
	ts.cliB.Close()
}

func (ts *clusterTestSuite) TestElection() {
	ctx := context.Background()

	eleA := ts.cliA.NewElection(ctx, ts.pref1)
	role, err := eleA.Campaign(ctx, ts.valA)
	ts.Nil(err)
	ts.Equal(RoleLeader, role)

	eleB := ts.cliB.NewElection(ctx, ts.pref1)

	roleB, err := eleB.Campaign(ctx, ts.valB)
	ts.Nil(err)
	ts.Equal(RoleFollower, roleB)

	ts.cliA.Close()

	roleB, err = eleB.Campaign(ctx, ts.valB)
	ts.Nil(err)
	ts.Equal(RoleLeader, roleB)

}

func (ts *clusterTestSuite) TestSvcDs() {
	ctx := context.Background()

	svcs := []string{"A", "B"}
	path := "/testsvc/"
	ts.Nil(ts.cliA.Register(ctx, path+svcs[0], svcs[0]))
	ts.Nil(ts.cliA.Register(ctx, path+svcs[1], svcs[1]))

	acts, err := ts.cliB.Discovery(ctx, path)
	ts.Nil(err)

	ts.Equal(svcs, acts)
}

func (ts *clusterTestSuite) TestSvcLease() {
	ctx := context.Background()

	svcs := []string{"A", "B"}
	path := "/testsvc/"
	ts.Nil(ts.cliA.Register(ctx, path+svcs[0], svcs[0]))
	ts.Nil(ts.cliB.Register(ctx, path+svcs[1], svcs[1]))

	ts.cliA.Close()

	acts, err := ts.cliB.Discovery(ctx, path)
	ts.Nil(err)

	ts.Equal(1, len(acts))
	ts.Equal(svcs[1], acts[0])
}
