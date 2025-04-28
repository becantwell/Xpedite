"""
Utility functions for testing Jupyter Notebook functionality

Author: Brooke Elizabeth Cantwell, Morgan Stanley
"""

from xpedite.analytics.timelineFilter import locateTimeline

CATEGORY = ''
ROOT_NODE_NAME = 'root'

def validateRoute(scenarios, route):
  """
  Validate transaction route against probes in a scenario
  """
  assert len(route) >= 1
  for point in route.points:
    assert point in [probe.sysName for probe in scenarios.probes]


def validateTimelines(scenarios, profiles, txnCount):
  """
  Validate locating timelines and negative cases with out of bounds timeline IDs
  """
  for txnNum in range(1, txnCount):
    timeline = locateTimeline(profiles, txnNum)
    assert timeline
    validateTransaction(scenarios, timeline.txn, txnNum)
  assert not locateTimeline(profiles, (txnCount * 2) + 1) # test negative case

def validateTransaction(scenarios, txn, txnNum):
  """
  Validate transaction with a transaction's counters against probes
  """
  assert txn.txnId == txnNum
  for counter in txn.counters:
    assert counter.probe.sysName in [probe.sysName for probe in scenarios.probes]
  assert [probe in txn.probeMap for probe in scenarios.probes]
  validateRoute(scenarios, txn.route)
  assert txn.hasEndProbe

def validateTimelinePlot(profiles, txnId):
  """
  Validate logic to build transaction trees and plots
  """
  from xpedite.jupyter.plot           import buildTxnPlot, buildTxnPlotTree
  from xpedite.analytics.timelineTree import buildTimelineTree
  timeline = locateTimeline(profiles, txnId)
  assert buildTxnPlot(timeline)
  timelineTree = buildTimelineTree(timeline)
  assert timelineTree.name == ROOT_NODE_NAME
  assert len(timelineTree.children) >= 1
  assert buildTxnPlotTree(timelineTree)

def validateDeltaSeriesRepo(profile):
  """
  Validate a delta series repo against a profile's delta series collection values
  """
  if len(profile.current.deltaSeriesRepo) > 1:
    for eventName, deltaSeriesCollection in profile.current.deltaSeriesRepo.iteritems():
      if profile.benchmarks:
        for benchmark in profile.benchmarks.values():
          deltaSeriesCollection = benchmark.deltaSeriesRepo.get(eventName, None)
          assert deltaSeriesCollection
  else:
    deltaSeriesCollection = profile.current.getTscDeltaSeriesCollection()
    assert deltaSeriesCollection
  return deltaSeriesCollection

def validateDeltaSeries(profile):
  """
  Validate a conflated timeline against a delta series repo created with that timeline
  as part of a profile
  """
  deltaSeriesRepo = profile.current.deltaSeriesRepo
  assert deltaSeriesRepo
  count = 0
  for _, deltaSeries in deltaSeriesRepo.iteritems():
    timeline = profile.current.timelineCollection[count]
    assert timeline
    for j in range(0, len(timeline) - 1):
      assert deltaSeries[j][count] == timeline[j].duration
    count = count + 1

def validateConflateRoutes(scenarios, profiles, route):
  """
  Validate conflating of routes by removing probes from the route and
  validating the new route and transaction against delta series
  """
  import random
  from xpedite.types.route         import Route, conflateRoutes
  from xpedite.analytics.conflator import Conflator
  dstRouteProbes = list(route.probes)
  while len(dstRouteProbes) > 2:
    del dstRouteProbes[random.randint(0, len(dstRouteProbes) - 1)]
    dstRoute = Route(dstRouteProbes)
    conflatedRoute = Route([route.probes[i] for i in conflateRoutes(route, dstRoute)])
    assert conflatedRoute
    validateRoute(scenarios, conflatedRoute)
    assert conflatedRoute == dstRoute
    dstProfile = Conflator().conflateProfiles(profiles, conflatedRoute, CATEGORY)
    assert dstProfile
    return dstProfile

def validateConflation(scenarios, profiles):
  """
  Start validation with routes
  """
  validateTimelinePlot(profiles, 1)
  for profile in profiles:
    for timeline in profile.current.timelineCollection:
      dstProfile = validateConflateRoutes(scenarios, profiles, timeline.txn.route)
    validateDeltaSeries(dstProfile)
    validateDeltaSeriesRepo(profile)
