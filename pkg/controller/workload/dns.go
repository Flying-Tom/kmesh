package workload

import (
	"net"
	"net/netip"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	"kmesh.net/kmesh/api/v2/workloadapi"
	"kmesh.net/kmesh/pkg/controller/workload/cache"
	"kmesh.net/kmesh/pkg/dns"
	// "kmesh.net/kmesh/pkg/logger"
)

type dnsController struct {
	servicesChan chan []*workloadapi.Service
	cache        cache.ServiceCache
	dnsResolver  *dns.DNSResolver
	// store the copy of pendingResolveWorkload.
	serviceCache map[string]*pendingResolveDomain
	// store all pending hostnames in the workloads
	pendingHostnames map[string][]string
	sync.RWMutex
}

// pending resolve domain info of Dual-Engine Mode,
// workload is used for create the apiworkload
type pendingResolveDomain struct {
	Services    []*workloadapi.Service
	RefreshRate time.Duration
}

func NewDnsController(serviceCache cache.ServiceCache) (*dnsController, error) {
	resolver, err := dns.NewDNSResolver()
	if err != nil {
		return nil, err
	}
	return &dnsController{
		servicesChan:     make(chan []*workloadapi.Service),
		cache:            serviceCache,
		dnsResolver:      resolver,
		serviceCache:     make(map[string]*pendingResolveDomain),
		pendingHostnames: make(map[string][]string),
	}, nil
}

func (r *dnsController) Run(stopCh <-chan struct{}) {
	go r.dnsResolver.StartDnsResolver(stopCh)
	go r.refreshWorker(stopCh)
	go r.processServices()
	go func() {
		<-stopCh
		close(r.servicesChan)
	}()
}

func (r *dnsController) processServices() {
	for services := range r.servicesChan {
		r.processDomains(services)
	}
}

func (r *dnsController) processDomains(services []*workloadapi.Service) {
	domains := getPendingResolveDomain(services)

	// store all pending hostnames of clusters in pendingHostnames
	for _, service := range services {
		serviceName := service.GetName()
		info := []string{service.GetHostname()}
		r.pendingHostnames[serviceName] = info
	}

	// delete any scheduled re-resolve for domains we no longer care about
	r.dnsResolver.RemoveUnwatchDomain(domains)

	// update workloadCache with pendingResolveWorkload
	for k, v := range domains {
		addresses := r.dnsResolver.GetDNSAddresses(k)
		if addresses != nil {
			go r.updateServices(v.(*pendingResolveDomain), k, addresses)
		} else {
			domainInfo := &dns.DomainInfo{
				Domain:      k,
				RefreshRate: v.(*pendingResolveDomain).RefreshRate,
			}
			r.dnsResolver.AddDomainInQueue(domainInfo, 0)
		}
	}
}

func (r *dnsController) refreshWorker(stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		case domain := <-r.dnsResolver.DnsChan:
			pendingDomain := r.getServicesByDomain(domain)
			addrs := r.dnsResolver.GetDNSAddresses(domain)
			r.updateServices(pendingDomain, domain, addrs)
		}
	}
}

func (r *dnsController) updateServices(pendingDomain *pendingResolveDomain, domain string, addrs []string) {
	isServiceUpdate := false
	if pendingDomain == nil || addrs == nil {
		return
	}

	for _, service := range pendingDomain.Services {
		ready, newService := r.overwriteDnsService(service, domain, addrs)
		if ready {
			// 更新 workload 缓存和 BPF maps
			if r.cache.GetService(service.ResourceName()) != nil {
				r.cache.AddOrUpdateService(newService)
				isServiceUpdate = true
			}
		}
	}

	if isServiceUpdate {
		// w.cache.Flush() // 刷新到 BPF maps
	}
}

func (r *dnsController) overwriteDnsService(service *workloadapi.Service, domain string, addrs []string) (bool, *workloadapi.Service) {
	ready := true
	hostNames := r.pendingHostnames[service.GetName()]
	addressesOfHostname := make(map[string][]string)

	for _, hostName := range hostNames {
		addresses := r.dnsResolver.GetDNSAddresses(hostName)
		// There are hostnames in this Cluster that are not resolved.
		if addresses != nil {
			addressesOfHostname[hostName] = addresses
		} else {
			ready = false
		}
	}

	if ready {
		newService := cloneService(service)
		for _, addr := range addrs {
			if ip := net.ParseIP(addr); ip != nil && ip.To4() != nil {
				newService.Addresses = append(newService.Addresses, &workloadapi.NetworkAddress{
					Address: netip.MustParseAddr(addr).AsSlice(),
				})
			}
		}

		return ready, newService
	}

	return ready, nil
}

func getPendingResolveDomain(services []*workloadapi.Service) map[string]any {
	domains := make(map[string]any)

	for _, service := range services {
		hostname := service.GetHostname()
		if hostname == "" {
			continue
		}

		if _, err := netip.ParseAddr(hostname); err == nil {
			continue
		}

		if v, ok := domains[hostname]; ok {
			v.(*pendingResolveDomain).Services = append(v.(*pendingResolveDomain).Services, service)
		} else {

			domains[hostname] = &pendingResolveDomain{
				Services:    []*workloadapi.Service{service},
				RefreshRate: 15 * time.Second,
			}
		}
	}

	return domains
}

func (r *dnsController) getServicesByDomain(domain string) *pendingResolveDomain {
	r.RLock()
	defer r.RUnlock()

	if r.cache != nil {
		if v, ok := r.serviceCache[domain]; ok {
			return v
		}
	}
	return nil
}

func cloneService(service *workloadapi.Service) *workloadapi.Service {
	if service == nil {
		return nil
	}
	serviceCopy := proto.Clone(service).(*workloadapi.Service)
	return serviceCopy
}
