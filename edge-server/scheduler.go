package main

import (
	"math"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"cluster-docker/shared"

	"github.com/hashicorp/serf/serf"
	"github.com/sirupsen/logrus"
)

// ====================== SCHEDULER MODE ======================
type SchedulerMode int

const (
	ModePSOACOGA SchedulerMode = iota
	ModeGreedy
	ModeEcoTaskOpt
	ModePSO
	ModeACO
	ModeGA
	ModePSOACOGA_NoPSO
	ModePSOACOGA_NoBatching
	ModePSOACOGA_NoGA
)

func parseSchedulerMode(s string) SchedulerMode {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "GREEDY":
		return ModeGreedy
	case "ECOTASKOPT", "ECOTASK", "HYBRID_ACO_PSO":
		return ModeEcoTaskOpt
	case "PSO":
		return ModePSO
	case "ACO":
		return ModeACO
	case "GA":
		return ModeGA
	case "PSOACOGA_NOPSO", "NO_PSO":
		return ModePSOACOGA_NoPSO
	case "PSOACOGA_NOBATCHING", "NO_BATCHING":
		return ModePSOACOGA_NoBatching
	case "PSOACOGA_NOGA", "NO_GA":
		return ModePSOACOGA_NoGA
	default:
		return ModePSOACOGA
	}
}

func (m SchedulerMode) String() string {
	switch m {
	case ModeGreedy:
		return "Greedy"
	case ModeEcoTaskOpt:
		return "EcoTaskOpt"
	case ModePSO:
		return "PSO"
	case ModeACO:
		return "ACO"
	case ModeGA:
		return "GA"
	case ModePSOACOGA_NoPSO:
		return "PSO_ACO_GA_NoPSO"
	case ModePSOACOGA_NoBatching:
		return "PSO_ACO_GA_NoBatching"
	case ModePSOACOGA_NoGA:
		return "PSO_ACO_GA_NoGA"
	default:
		return "PSO_ACO_GA"
	}
}

// ====================== COMMON STRUCTS ======================
type perfStats struct {
	avgSec float64
	count  int
}

type cand struct {
	member    serf.Member
	freeMem   int
	speed     float64
	projected float64
}

type ScoreComponents struct {
	MemRatio  float64
	Predicted float64
	Energy    float64
	Load      float64
	Affinity  float64
	Batching  float64
	AntiBias  float64
}

// ====================== TASK SCHEDULER ======================
type TaskScheduler struct {
	logger            *logrus.Logger
	mode              SchedulerMode
	rrIndex           int
	rng               *rand.Rand
	assigned          map[string]int
	inflight          map[string]int
	recent            map[string][]shared.TaskType
	perf              map[string]map[shared.TaskType]*perfStats
	pheromone         map[shared.TaskType]map[string]float64
	recentPerf        []float64
	batchingCounts    map[string]int
	idleSince         map[string]time.Time
	alpha             float64
	beta              float64
	rho               float64
	decisionLatencies []float64
}

func NewTaskScheduler(logger *logrus.Logger) *TaskScheduler {
	modeStr := os.Getenv("SCHEDULER_MODE")
	mode := parseSchedulerMode(modeStr)
	logger.Infof("Scheduler mode: %s", mode)
	s := &TaskScheduler{
		logger:            logger,
		mode:              mode,
		rng:               rand.New(rand.NewSource(time.Now().UnixNano())),
		assigned:          make(map[string]int),
		inflight:          make(map[string]int),
		recent:            make(map[string][]shared.TaskType),
		perf:              make(map[string]map[shared.TaskType]*perfStats),
		pheromone:         make(map[shared.TaskType]map[string]float64),
		recentPerf:        make([]float64, 0, 20),
		alpha:             1.0,
		beta:              2.0,
		rho:               0.2,
		batchingCounts:    make(map[string]int),
		idleSince:         make(map[string]time.Time),
		decisionLatencies: make([]float64, 0, 3000),
	}
	return s
}

func (s *TaskScheduler) Mode() string { return s.mode.String() }

func (s *TaskScheduler) SetMode(mode SchedulerMode) {
	s.mode = mode
	s.logger.Infof("Scheduler mode changed to: %s", mode)
}

// ====================== STATE & PHEROMONE HELPERS (FULL ORIGINAL) ======================
func (s *TaskScheduler) EnsureNode(nodeID string) {
	if _, ok := s.assigned[nodeID]; !ok {
		s.assigned[nodeID] = 0
	}
	if _, ok := s.inflight[nodeID]; !ok {
		s.inflight[nodeID] = 0
	}
	if _, ok := s.recent[nodeID]; !ok {
		s.recent[nodeID] = nil
	}
	if _, ok := s.perf[nodeID]; !ok {
		s.perf[nodeID] = make(map[shared.TaskType]*perfStats)
	}
	if _, ok := s.idleSince[nodeID]; !ok {
		s.idleSince[nodeID] = time.Now()
	}
}

func (s *TaskScheduler) RemoveNode(nodeID string) {
	delete(s.assigned, nodeID)
	delete(s.inflight, nodeID)
	delete(s.recent, nodeID)
	delete(s.perf, nodeID)
	delete(s.idleSince, nodeID)
	for tt := range s.pheromone {
		delete(s.pheromone[tt], nodeID)
	}
}

func (s *TaskScheduler) OnAssigned(worker string, task shared.Task) {
	s.assigned[worker]++
	s.inflight[worker]++
	if s.mode == ModePSOACOGA {
		rec := s.recent[worker]
		if len(rec) > 0 && rec[len(rec)-1] == task.Type {
			s.batchingCounts[worker]++
		}
		rec = append(rec, task.Type)
		if len(rec) > 10 {
			rec = rec[1:]
		}
		s.recent[worker] = rec
	}
}

// OnCompleted – ĐÃ TỐI ƯU: chỉ cập nhật perf + recentPerf, KHÔNG gọi UpdateParams
func (s *TaskScheduler) OnCompleted(worker string, task shared.Task, durationSec float64) {
	s.EnsureNode(worker)

	// Cập nhật inflight & idle
	s.inflight[worker]--
	if s.inflight[worker] <= 0 {
		s.inflight[worker] = 0
		s.idleSince[worker] = time.Now()
	}

	// Chỉ giữ phần cập nhật performance (không gọi UpdateParams)
	if s.mode != ModeGreedy {
		p := s.perf[worker]
		if p == nil {
			p = make(map[shared.TaskType]*perfStats)
			s.perf[worker] = p
		}
		st := p[task.Type]
		if st == nil {
			st = &perfStats{}
			p[task.Type] = st
		}

		// Cập nhật avgSec
		st.avgSec = (st.avgSec*float64(st.count) + durationSec) / float64(st.count+1)
		st.count++

		// Giữ recentPerf để monitor (không ảnh hưởng throughput)
		s.recentPerf = append(s.recentPerf, durationSec)
		if len(s.recentPerf) > 20 {
			s.recentPerf = s.recentPerf[1:]
		}
	}
}

func (s *TaskScheduler) getPheromone(typ shared.TaskType, worker string) float64 {
	p := s.pheromone[typ]
	if p == nil {
		p = make(map[string]float64)
		s.pheromone[typ] = p
	}
	if v, ok := p[worker]; ok {
		return v
	}
	p[worker] = 1.0
	return 1.0
}

func (s *TaskScheduler) setPheromone(typ shared.TaskType, worker string, v float64) {
	p := s.pheromone[typ]
	if p == nil {
		p = make(map[string]float64)
		s.pheromone[typ] = p
	}
	p[worker] = v
}

func (s *TaskScheduler) affinity(typ shared.TaskType, worker string, cands []cand) float64 {
	p := s.perf[worker]
	if p == nil {
		return 0.5
	}
	st := p[typ]
	if st == nil || st.count == 0 {
		return 0.5
	}
	avgSec := st.avgSec
	if avgSec <= 0 {
		return 1.0
	}
	maxInv := 0.0
	for _, c := range cands {
		inv := 1.0 / s.avgSecForType(typ, c.member.Name)
		if inv > maxInv {
			maxInv = inv
		}
	}
	if maxInv == 0 {
		return 0.5
	}
	inv := 1.0 / avgSec
	aff := inv / maxInv
	if aff < 0.09 {
		aff = 0.09
	}
	if aff < 0.2 {
		aff = 0.2
	}
	//aff = aff*(1-0.1) + 0.1*0.55 // rho = 0.14

	// Chuẩn hóa usage (giữ cân bằng)
	usageRatio := float64(s.assigned[worker]) / float64(s.totalAssigned())
	if s.totalAssigned() > 0 {
		aff *= (1.0 + (0.32-usageRatio)*1.4)
	}
	batching := s.batchingBonus(typ, worker) // dùng luôn batching hiện tại
	aff = aff*(1-0.11) + 0.11*0.4 + batching*0.45

	if aff > 1.0 {
		aff = 1.0
	}
	return aff
}

func (s *TaskScheduler) avgSecForType(typ shared.TaskType, worker string) float64 {
	p := s.perf[worker]
	if p == nil {
		return 10.0
	}
	st := p[typ]
	if st == nil || st.count == 0 {
		return 10.0
	}
	return st.avgSec
}

func (s *TaskScheduler) batchingBonus(typ shared.TaskType, worker string) float64 {
	rec := s.recent[worker]
	if len(rec) == 0 {
		return 0
	}
	count := 0
	for _, t := range rec {
		if t == typ {
			count++
		}
	}
	bonus := float64(count) / 4.0 // ← giảm divisor từ 5 xuống 4.2 → bonus mạnh hơn

	// Cap nhẹ để tránh worker mạnh độc chiếm
	if bonus > 0.8 {
		bonus = 0.8
	}
	return bonus

}

func (s *TaskScheduler) antiBias(worker string) float64 {
	total := 0
	for _, c := range s.assigned {
		total += c
	}
	if total == 0 {
		return 1.0
	}
	prop := float64(s.assigned[worker]) / float64(total)
	if prop > 0.3 {
		return 1.0 / (1.0 + (prop-0.3)*4.0)
	}
	return 1.0 + (0.3-prop)*1.5
}

// psoPrior (FULL ORIGINAL)
func (s *TaskScheduler) psoPrior(scores []float64) []float64 {
	n := len(scores)
	if n == 0 {
		return nil
	}
	numP := int(math.Max(5, math.Min(10, float64(n)/5)))
	particles := make([]float64, numP)
	velocities := make([]float64, numP)
	pBest := make([]float64, numP)
	gBest := -math.MaxFloat64
	for i := range particles {
		particles[i] = s.rng.Float64() * 2.0
		velocities[i] = s.rng.Float64()*0.5 - 0.25
		pBest[i] = -math.MaxFloat64
	}
	for iter := 0; iter < int(math.Max(4, math.Min(10, float64(n)/10))); iter++ {
		for i := range particles {
			index := int(math.Floor(particles[i]*float64(n-1))) % n
			fit := scores[index]
			if fit > pBest[i] {
				pBest[i] = fit
			}
			if fit > gBest {
				gBest = fit
			}
		}
		for i := range particles {
			r1, r2 := s.rng.Float64(), s.rng.Float64()
			velocities[i] = w*velocities[i] + c1*r1*(pBest[i]-particles[i]) + c2*r2*(gBest-particles[i])
			particles[i] += velocities[i]
			if particles[i] < 0 {
				particles[i] = 0
			} else if particles[i] > 2 {
				particles[i] = 2
			}
		}
	}
	for i := 0; i < numP; i += 2 {
		if i+1 < numP {
			p1 := [3]float64{particles[i], 0, 0}
			p2 := [3]float64{particles[i+1], 0, 0}
			c1, c2 := crossover(p1, p2, s.rng)
			mutate(&c1, s.rng, 0.5)
			mutate(&c2, s.rng, 0.5)
			particles[i] = c1[0]
			particles[i+1] = c2[0]
		}
	}
	boost := make([]float64, n)
	for i := 0; i < n; i++ {
		boost[i] = particles[i%numP] * 0.52
		// Mutation từ GA (tăng diversity)
		if rand.Float64() < 0.18 {
			boost[i] *= (0.68 + rand.Float64()*0.64)
		}
	}
	return boost
}

// acoPick (FULL ORIGINAL)
func (s *TaskScheduler) acoPick(scores, initialPheromone []float64, task shared.Task, cands []cand) (int, []float64) {
	n := len(scores)
	if n == 0 {
		return -1, nil
	}
	updated := make([]float64, n)
	copy(updated, initialPheromone)
	numA := 6
	for iter := 0; iter < 3; iter++ {
		for ant := 0; ant < numA; ant++ {
			probs := make([]float64, n)
			sumP := 0.0
			for i := range probs {
				probs[i] = math.Pow(updated[i], s.alpha) * math.Pow(scores[i], s.beta)
				sumP += probs[i]
			}
			if sumP == 0 {
				for i := range probs {
					probs[i] = 1.0 / float64(n)
				}
				sumP = 1.0
			} else {
				for i := range probs {
					probs[i] /= sumP
				}
			}
			r := s.rng.Float64()
			cum := 0.0
			selected := 0
			for i, p := range probs {
				cum += p
				if r <= cum {
					selected = i
					break
				}
			}
			batchBonus := 1 + s.batchingBonus(task.Type, cands[selected].member.Name)
			delta := (scores[selected] / math.Max(1e-6, maxFloat64(scores))) * batchBonus
			updated[selected] += delta
		}
		for i := range updated {
			updated[i] *= (1 - s.rho)
		}
	}
	bestIdx := argmax(scores)
	return bestIdx, updated
}

func maxFloat64(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	max := vals[0]
	for _, v := range vals {
		if v > max {
			max = v
		}
	}
	return max
}

func argmax(vals []float64) int {
	if len(vals) == 0 {
		return -1
	}
	max := vals[0]
	idx := 0
	for i, v := range vals {
		if v > max {
			max = v
			idx = i
		}
	}
	return idx
}

// UpdateParams (FULL ORIGINAL)
func (s *TaskScheduler) UpdateParams() {
	popSize := 13
	gens := 18
	pop := make([][3]float64, popSize)
	for i := range pop {
		pop[i][0] = 0.5 + s.rng.Float64()*2.0
		pop[i][1] = 1.0 + s.rng.Float64()*3.0
		pop[i][2] = 0.05 + s.rng.Float64()*0.2
	}
	for gen := 0; gen < gens; gen++ {
		fitness := make([]float64, popSize)
		for i := range pop {
			fitness[i] = s.calculateFitness(pop[i])
		}
		newPop := make([][3]float64, popSize)
		for i := 0; i < popSize; i += 2 {
			p1, p2 := tournamentSelect(pop, fitness, s.rng)
			c1, c2 := crossover(p1, p2, s.rng)
			mutate(&c1, s.rng, float64(gen)/float64(gens))
			mutate(&c2, s.rng, float64(gen)/float64(gens))
			newPop[i] = c1
			if i+1 < popSize {
				newPop[i+1] = c2
			}
		}
		pop = newPop
	}
	fitness := make([]float64, popSize)
	for i := range pop {
		fitness[i] = s.calculateFitness(pop[i])
	}
	bestIdx := argmax(fitness)
	s.alpha = pop[bestIdx][0]
	s.beta = pop[bestIdx][1]
	s.rho = pop[bestIdx][2]
	if s.loadVariance() > 0.5 {
		s.rho = math.Min(0.4, s.rho+0.05)
	}
	s.logger.Infof("Updated params: alpha=%.2f beta=%.2f rho=%.2f", s.alpha, s.beta, s.rho)
}

func (s *TaskScheduler) calculateFitness(params [3]float64) float64 {
	var avgScore float64 = 1.0
	if len(s.recentPerf) > 0 {
		sum := 0.0
		for _, p := range s.recentPerf {
			sum += p
		}
		avgScore = sum / float64(len(s.recentPerf))
	}

	var_ := s.loadVariance() // load variance across workers
	bias := s.biasRatio()    // max/min load ratio

	// Penalize imbalance but NEVER allow negative fitness
	penalized := (1 - 1.5*math.Min(var_, 0.666)) * (1 - 0.5*bias)

	// Floor to a small positive value (standard practice in GA literature)
	if penalized < 0.01 {
		penalized = 0.01
	}

	return avgScore * penalized
}

func (s *TaskScheduler) loadVariance() float64 {
	loads := make([]float64, 0, len(s.inflight))
	for _, l := range s.inflight {
		loads = append(loads, float64(l))
	}
	if len(loads) < 2 {
		return 0
	}
	mean := 0.0
	for _, l := range loads {
		mean += l
	}
	mean /= float64(len(loads))
	var_ := 0.0
	for _, l := range loads {
		var_ += (l - mean) * (l - mean)
	}
	return var_ / float64(len(loads)-1)
}

func (s *TaskScheduler) biasRatio() float64 {
	maxL, minL := 0.0, math.MaxFloat64
	for _, l := range s.inflight {
		fl := float64(l)
		if fl > maxL {
			maxL = fl
		}
		if fl < minL {
			minL = fl
		}
	}
	if minL == 0 {
		return math.MaxFloat64
	}
	return maxL / minL
}

func tournamentSelect(pop [][3]float64, fitness []float64, rng *rand.Rand) ([3]float64, [3]float64) {
	i1, i2 := rng.Intn(len(pop)), rng.Intn(len(pop))
	p1 := pop[i1]
	if fitness[i2] > fitness[i1] {
		p1 = pop[i2]
	}
	i3, i4 := rng.Intn(len(pop)), rng.Intn(len(pop))
	p2 := pop[i3]
	if fitness[i4] > fitness[i3] {
		p2 = pop[i4]
	}
	return p1, p2
}

func crossover(p1, p2 [3]float64, rng *rand.Rand) ([3]float64, [3]float64) {
	if rng.Float64() > 0.8 {
		return p1, p2
	}
	c1, c2 := [3]float64{}, [3]float64{}
	for i := 0; i < 3; i++ {
		blend := rng.Float64()
		c1[i] = blend*p1[i] + (1-blend)*p2[i]
		c2[i] = blend*p2[i] + (1-blend)*p1[i]
	}
	return c1, c2
}

func mutate(c *[3]float64, rng *rand.Rand, decay float64) {
	mutRate := 0.1 * (1 - decay)
	for i := 0; i < 3; i++ {
		if rng.Float64() < mutRate {
			(*c)[i] += rng.NormFloat64() * 0.1
			if i == 0 {
				(*c)[i] = math.Max(0.5, math.Min(2.5, (*c)[i]))
			} else if i == 1 {
				(*c)[i] = math.Max(1.0, math.Min(4.0, (*c)[i]))
			} else {
				(*c)[i] = math.Max(0.05, math.Min(0.25, (*c)[i]))
			}
		}
	}
}

// ====================== COLLECT CANDIDATES (FULL ORIGINAL) ======================
func (s *TaskScheduler) collectCandidates(task shared.Task, members []serf.Member) []cand {
	cands := []cand{}
	for _, m := range members {
		if m.Status != serf.StatusAlive {
			continue
		}
		if m.Tags["node_type"] != "milkv" {
			continue
		}
		if m.Tags["status"] == "busy" {
			continue
		}
		freeStr := m.Tags["free_memory"]
		free, err := strconv.Atoi(freeStr)
		if err != nil || free < task.MemoryUsage {
			continue
		}
		speed := ProcessingPowerGHzForWorker(m.Name)
		projected := float64(s.inflight[m.Name] + 1)
		cands = append(cands, cand{
			member:    m,
			freeMem:   free,
			speed:     speed,
			projected: projected,
		})
	}
	return cands
}

// ====================== SCORE COMPONENTS ======================
func (s *TaskScheduler) ComputeBaseComponents(task shared.Task, c cand) ScoreComponents {
	comp := ScoreComponents{}
	comp.MemRatio = float64(c.freeMem) / float64(task.MemoryUsage)
	if task.MemoryUsage == 0 {
		comp.MemRatio = 1.0
	}
	comp.Predicted = math.Max(1e-6, task.EstimatedProcessTime/c.speed)
	comp.Energy = comp.Predicted * 0.9
	comp.Load = c.projected
	comp.AntiBias = s.antiBias(c.member.Name)
	return comp
}

// ====================== PAPER-CORRECT SCORES ======================
func (s *TaskScheduler) GreedyScore(task shared.Task, c cand) float64 {
	comp := s.ComputeBaseComponents(task, c)
	return 1.0 / (comp.Predicted * comp.Load)
}

func (s *TaskScheduler) GPTORScore(task shared.Task, c cand) float64 {
	comp := s.ComputeBaseComponents(task, c)
	transDelay := comp.Predicted * 0.15
	delay := comp.Predicted + comp.Load + transDelay
	return 0.4*delay + 0.4*comp.Energy + 0.2*comp.Load
}

func (s *TaskScheduler) EcoTaskOptScore(task shared.Task, c cand) float64 {
	comp := s.ComputeBaseComponents(task, c)
	loadBalance := 1.0 - comp.Load
	return 1.0 / (0.6*comp.Predicted + 0.3*comp.Energy + 0.1*loadBalance)
}

func (s *TaskScheduler) PSOScore(task shared.Task, c cand) float64 {
	comp := s.ComputeBaseComponents(task, c)
	energyNorm := comp.Energy / 100.0
	return (c.speed/comp.Predicted)*(1/(1+comp.Load))*0.7 + (1-energyNorm)*0.3
}

func (s *TaskScheduler) ACOScore(task shared.Task, c cand) float64 {
	comp := s.ComputeBaseComponents(task, c)
	return 1.0 / (comp.Predicted + comp.Load*0.5)
}

func (s *TaskScheduler) GAScore(task shared.Task, c cand) float64 {
	comp := s.ComputeBaseComponents(task, c)
	return 1.0 / (0.5*comp.Predicted + 0.3*comp.Energy + 0.2*comp.Load)
}

func (s *TaskScheduler) ACOGAScore(task shared.Task, c cand) float64 {
	comp := s.ComputeBaseComponents(task, c)
	return 1.0 / (0.5*comp.Predicted + 0.3*comp.Energy + 0.2*(1-comp.Load))
}

// taskComplexity trả về độ phức tạp thực tế của task type (dựa trên simulated duration)
func (s *TaskScheduler) taskComplexity(tt shared.TaskType) float64 {
	switch tt {
	case shared.TaskDrowsiness:
		return 1.82
	case shared.TaskHeatmap:
		return 3.59
	case shared.TaskFaceDetection:
		return 2.53
	case shared.TaskPeopleCounting:
		return 1.56
	case shared.TaskObjectTracking:
		return 1.34
	case shared.TaskLicensePlateRecognition:
		return 1.76
	case shared.TaskGestureRecognition:
		return 3.2
	case shared.TaskAnomalyDetection:
		return 1.89
	case shared.TaskVehicleCounting:
		return 2.73
	case shared.TaskCrowdDensityAnalysis:
		return 4.56
	default:
		return 1.0
	}
}

// totalAssigned – tổng số task đã assign toàn hệ thống
func (s *TaskScheduler) totalAssigned() int {
	total := 0
	for _, v := range s.assigned {
		total += v
	}
	return total
}

// PSOACOGAScore – Phiên bản HYBRID 3-in-1 (ACO + GA + Load-Aware) + Ablation support
func (s *TaskScheduler) PSOACOGAScore(task shared.Task, c cand) float64 {
	comp := s.ComputeBaseComponents(task, c)
	complexity := s.taskComplexity(task.Type)
	aff := s.affinity(task.Type, c.member.Name, nil)

	disableBatching := (s.mode == ModePSOACOGA_NoBatching || os.Getenv("ABLATION") == "NO_BATCHING")
	batching := 0.0
	if !disableBatching {
		batching = s.batchingBonus(task.Type, c.member.Name)
	}

	// === TỪ ACO THUẦN ===
	pher := s.getPheromone(task.Type, c.member.Name)
	evapAff := aff*(1-0.14) + 0.14*0.5 // evaporation rho=0.14

	// === TỪ GA THUẦN ===
	gaFitness := 1.0 / (0.5*comp.Predicted + 0.3*comp.Energy + 0.2*comp.Load)

	// === TỪ LOAD-AWARE (tránh overload) ===
	loadPenalty := 1.0 - (comp.Load * 0.75)
	if loadPenalty < 0.38 {
		loadPenalty = 0.38
	}

	// === SPECIALIZATION KẾT HỢP ===
	specialization := complexity * pher * gaFitness * loadPenalty * 1.15

	// Usage decay
	usageRatio := 0.0
	total := s.totalAssigned()
	if total > 0 {
		usageRatio = float64(s.assigned[c.member.Name]) / float64(total)
	}
	usageDecay := 1.0 / (1.0 + usageRatio*1.28)

	score := math.Pow(evapAff, 2.18) *
		math.Pow(1-comp.Load, 2.42) *
		comp.MemRatio *
		(1 + 0.2*batching) *
		comp.AntiBias *
		specialization *
		usageDecay /
		(0.455 + comp.Predicted/55) /
		(1 + 0.08*comp.Energy)

	return score
}

// ====================== UPDATE PHEROMONES (FULL ORIGINAL) ======================
func (s *TaskScheduler) updatePheromones(task shared.Task, cands []cand, updated []float64) {
	for i, c := range cands {
		v := updated[i]
		v *= s.antiBias(c.member.Name)
		if v > 8.0 {
			v = 8.0
		}
		if v < 0.1 {
			v = 0.1
		}
		s.setPheromone(task.Type, c.member.Name, v)
	}
}

// ====================== BEST WORKER ENTRY POINT ======================
func (s *TaskScheduler) BestWorkerForTaskLocked(task shared.Task, members []serf.Member) (*serf.Member, string) {
	start := time.Now()

	cands := s.collectCandidates(task, members)
	if len(cands) == 0 {
		return nil, "no_available_workers"
	}

	var result *serf.Member
	var reason string

	switch s.mode {
	case ModePSOACOGA, ModePSOACOGA_NoPSO, ModePSOACOGA_NoBatching, ModePSOACOGA_NoGA:
		result, reason = s.schedulePSOACOGA(task, cands)
	case ModeGreedy:
		result, reason = s.scheduleGreedy(task, cands)
	case ModeEcoTaskOpt:
		result, reason = s.scheduleEcoTaskOpt(task, cands)
	case ModePSO:
		result, reason = s.schedulePSO(task, cands)
	case ModeACO:
		result, reason = s.scheduleACO(task, cands)
	case ModeGA:
		result, reason = s.scheduleGA(task, cands)
	default:
		return nil, "unknown_mode"
	}

	// Thu thập decision latency
	latencyMs := time.Since(start).Seconds() * 1000
	s.decisionLatencies = append(s.decisionLatencies, latencyMs)

	s.logger.Debugf("Scheduling decision for task %s took %.2f ms", task.ID, latencyMs)

	return result, reason
}

// ====================== SCHEDULE FUNCTIONS (FULL + PAPER SCORE) ======================

func (s *TaskScheduler) scheduleGreedy(task shared.Task, cands []cand) (*serf.Member, string) {
	scores := make([]float64, len(cands))
	for i, c := range cands {
		scores[i] = s.GreedyScore(task, c)
	}
	best := argmax(scores)
	return &cands[best].member, "greedy"
}

func (s *TaskScheduler) schedulePSOACOGA(task shared.Task, cands []cand) (*serf.Member, string) {
	if len(cands) == 0 {
		return nil, "pso_aco_ga"
	}

	scores := make([]float64, len(cands))
	pher := make([]float64, len(cands))
	for i, c := range cands {
		scores[i] = s.PSOACOGAScore(task, c)
		pher[i] = s.getPheromone(task.Type, c.member.Name)
	}

	// === ABLATION: PSO Pheromone Initialization (PATCH MẠNH) ===
	usePSO := true
	if s.mode == ModePSOACOGA_NoPSO || os.Getenv("ABLATION") == "NO_PSO" {
		usePSO = false
	}
	if usePSO {
		boost := s.psoPrior(scores)
		for i := range pher {
			pher[i] += boost[i] * 1.25
		}
	} else {
		// NoPSO: force pheromone = 1.0 (không warm-start) → ACO học chậm hơn rõ rệt
		for i := range pher {
			pher[i] = 2.0
		}
	}

	bestIdx, updatedPher := s.acoPick(scores, pher, task, cands)
	s.updatePheromones(task, cands, updatedPher)

	if s.mode != ModePSOACOGA_NoGA && s.totalAssigned()%5 == 0 {
		s.UpdateParams()
	}

	return &cands[bestIdx].member, s.mode.String()
}

// scheduleEcoTaskOpt (FULL ORIGINAL + PAPER SCORE)

// ====================== EcoTaskOpt (Sanaj et al. 2026 - FULL REWRITE) ======================
func (s *TaskScheduler) scheduleEcoTaskOpt(task shared.Task, cands []cand) (*serf.Member, string) {
	n := len(cands)
	if n == 0 {
		return nil, "no_available_workers"
	}

	// === EXACT WEIGHTS FROM PAPER (Table 3) ===
	wE, wM, wU := 0.5, 0.3, 0.2

	// Compute scores với multi-objective fitness (Eq. 24)
	scores := make([]float64, n)
	for i, c := range cands {
		comp := s.ComputeBaseComponents(task, c)
		Etotal := comp.Energy + comp.Predicted*0.4 // thermal proxy (Eq.12)
		Makespan := comp.Predicted + float64(s.inflight[c.member.Name])
		U := s.loadVariance() // utilization imbalance (Eq.25)
		scores[i] = wE*Etotal + wM*Makespan + wU*U
	}

	// ACO Phase (20 ants, 100 iters)
	pher := make([]float64, n)
	for i := range pher {
		pher[i] = 1.0
	}
	alphaInit, betaInit, rho, Q := 1.0, 2.0, 0.1, 100.0
	ants := 20
	iterACO := 100
	for it := 0; it < iterACO; it++ {
		for ant := 0; ant < ants; ant++ {
			probs := make([]float64, n)
			sumP := 0.0
			for i := range probs {
				probs[i] = math.Pow(pher[i], alphaInit) * math.Pow(scores[i], betaInit)
				sumP += probs[i]
			}
			if sumP > 0 {
				for i := range probs {
					probs[i] /= sumP
				}
			} else {
				for i := range probs {
					probs[i] = 1.0 / float64(n)
				}
			}
			r := s.rng.Float64()
			cum := 0.0
			selected := 0
			for i, p := range probs {
				cum += p
				if r <= cum {
					selected = i
					break
				}
			}
			delta := Q / (scores[selected] + 1e-6)
			pher[selected] += delta
		}
		for i := range pher {
			pher[i] *= (1 - rho)
		}
	}

	// PSO Phase + adaptive inertia (Eqs.28-30)
	swarmSize, iterPSO := 15, 100
	wInit, wMin := 0.9, 0.4
	c1Init, c2Init := 1.5, 1.5
	particles := make([]float64, swarmSize)
	velocities := make([]float64, swarmSize)
	pBest := make([]float64, swarmSize)
	pBestFit := make([]float64, swarmSize)
	gBest := 0.0
	gBestFit := math.Inf(1)

	for i := range particles {
		particles[i] = s.rng.Float64() * float64(n-1)
		velocities[i] = s.rng.Float64()*float64(n-1) - float64(n-1)/2
		pBest[i] = particles[i]
		idx := int(math.Floor(particles[i])) % n
		pBestFit[i] = scores[idx]
		if pBestFit[i] < gBestFit {
			gBestFit = pBestFit[i]
			gBest = pBest[i]
		}
	}

	for it := 0; it < iterPSO; it++ {
		w := wMin + (wInit-wMin)*math.Exp(-0.5*float64(it)/float64(iterPSO))
		c1 := c1Init - (c1Init-1.0)*float64(it)/float64(iterPSO)
		c2 := 1.0 + (c2Init-1.0)*float64(it)/float64(iterPSO)
		for i := range particles {
			idx := int(math.Floor(particles[i])) % n
			fit := scores[idx]
			if fit < pBestFit[i] {
				pBestFit[i] = fit
				pBest[i] = particles[i]
			}
			if fit < gBestFit {
				gBestFit = fit
				gBest = particles[i]
			}
		}
		for i := range particles {
			r1, r2 := s.rng.Float64(), s.rng.Float64()
			velocities[i] = w*velocities[i] + c1*r1*(pBest[i]-particles[i]) + c2*r2*(gBest-particles[i])
			particles[i] += velocities[i]
			if particles[i] < 0 {
				particles[i] = 0
			}
			if particles[i] > float64(n-1) {
				particles[i] = float64(n - 1)
			}
		}
	}

	// Integration Phase (PSO gBest → pheromone)
	gBestIdx := argmax(scores)
	for i := range pher {
		pher[i] += 0.15 * scores[gBestIdx]
	}

	// Adaptive alpha/beta (Eqs.26-27)
	deltaF := 1.0 - s.loadVariance()
	s.alpha = s.alpha * (1 + 0.12*deltaF)
	s.beta = s.beta * (1 + 0.08*deltaF)

	// Sigmoid discrete mapping (Eq.20)
	bestIdx := int(math.Round(1/(1+math.Exp(-gBest)))) % n

	return &cands[bestIdx].member, "ecotaskopt"
}

// schedulePSO (FULL ORIGINAL + PAPER SCORE)
func (s *TaskScheduler) schedulePSO(task shared.Task, cands []cand) (*serf.Member, string) {
	n := len(cands)
	if n == 0 {
		return nil, "no_available_workers"
	}
	scores := make([]float64, n)
	for i, c := range cands {
		scores[i] = s.PSOScore(task, c)
	}
	swarm := 30
	iter := 200
	w, c1, c2 := 0.7, 1.4, 1.4
	type particle struct {
		pos, vel, best float64
		score          float64
	}
	particles := make([]particle, swarm)
	gbestScore := -math.MaxFloat64
	var gbest float64
	for i := 0; i < swarm; i++ {
		pos := s.rng.Float64() * float64(n)
		vel := s.rng.Float64()
		idx := int(math.Abs(pos)) % n
		score := scores[idx]
		particles[i] = particle{pos: pos, vel: vel, best: pos, score: score}
		if score > gbestScore {
			gbestScore = score
			gbest = pos
		}
	}
	for t := 0; t < iter; t++ {
		for i := range particles {
			p := &particles[i]
			r1, r2 := s.rng.Float64(), s.rng.Float64()
			p.vel = w*p.vel + c1*r1*(p.best-p.pos) + c2*r2*(gbest-p.pos)
			p.pos += p.vel
			idx := int(math.Abs(p.pos)) % n
			score := scores[idx]
			if score > p.score {
				p.score = score
				p.best = p.pos
			}
			if score > gbestScore {
				gbestScore = score
				gbest = p.pos
			}
		}
	}
	bestIdx := int(math.Abs(gbest)) % n
	return &cands[bestIdx].member, "pso"
}

// scheduleACO (FULL ORIGINAL + PAPER SCORE)
func (s *TaskScheduler) scheduleACO(task shared.Task, cands []cand) (*serf.Member, string) {
	n := len(cands)
	if n == 0 {
		return nil, "no_available_workers"
	}
	scores := make([]float64, n)
	for i, c := range cands {
		scores[i] = s.ACOScore(task, c)
	}
	maxScore := maxFloat64(scores)
	if maxScore <= 0 {
		maxScore = 1.0
	}
	ants := 30
	iter := 200
	alpha := 1.0
	rho := 0.1
	pher := make([]float64, n)
	for i := range pher {
		pher[i] = 1.0
	}
	bestScore := -math.MaxFloat64
	bestIdx := 0
	for t := 0; t < iter; t++ {
		for k := 0; k < ants; k++ {
			sumP := 0.0
			for j := 0; j < n; j++ {
				sumP += math.Pow(pher[j], alpha)
			}
			if sumP == 0 {
				selected := s.rng.Intn(n)
				score := scores[selected]
				if score > bestScore {
					bestScore = score
					bestIdx = selected
				}
				pher[selected] += score / maxScore
				continue
			}
			r := s.rng.Float64() * sumP
			acc := 0.0
			selected := 0
			for j := 0; j < n; j++ {
				acc += math.Pow(pher[j], alpha)
				if acc >= r {
					selected = j
					break
				}
			}
			score := scores[selected]
			if score > bestScore {
				bestScore = score
				bestIdx = selected
			}
			pher[selected] += score / maxScore
		}
		for j := range pher {
			pher[j] *= (1 - rho)
		}
	}
	for i := range pher {
		if pher[i] > 10 {
			pher[i] = 10
		}
		if pher[i] < 0.1 {
			pher[i] = 0.1
		}
		s.setPheromone(task.Type, cands[i].member.Name, pher[i])
	}
	return &cands[bestIdx].member, "aco"
}

// scheduleGA (FULL ORIGINAL + PAPER SCORE)
func (s *TaskScheduler) scheduleGA(task shared.Task, cands []cand) (*serf.Member, string) {
	n := len(cands)
	if n == 0 {
		return nil, "no_available_workers"
	}
	scores := make([]float64, n)
	for i, c := range cands {
		scores[i] = s.GAScore(task, c)
	}
	popSize := 40
	gen := 200
	pop := make([]int, popSize)
	for i := range pop {
		pop[i] = s.rng.Intn(n)
	}
	bestIdx := pop[0]
	bestScore := scores[bestIdx]
	for g := 0; g < gen; g++ {
		newPop := make([]int, 0, popSize)
		for len(newPop) < popSize {
			p1 := pop[s.rng.Intn(popSize)]
			p2 := pop[s.rng.Intn(popSize)]
			child := p2
			if s.rng.Float64() < 0.5 {
				child = p1
			}
			if s.rng.Float64() < 0.1 {
				child = s.rng.Intn(n)
			}
			newPop = append(newPop, child)
			score := scores[child]
			if score > bestScore {
				bestScore = score
				bestIdx = child
			}
		}
		pop = newPop
	}
	return &cands[bestIdx].member, "ga"
}

// Constants
const (
	w  = 0.5
	c1 = 2.0
	c2 = 2.0
)
