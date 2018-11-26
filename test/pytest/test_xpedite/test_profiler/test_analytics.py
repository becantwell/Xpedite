"""
A pytest module to test analytics commands

Author: Brooke Elizabeth Cantwell, Morgan Stanley
"""

import pytest
from test_xpedite.test_profiler.profile     import runXpediteRecord
from test_xpedite.test_profiler.context     import Context
from test_xpedite.test_profiler.scenario    import ScenarioLoader, ScenarioType
from test_xpedite.test_profiler.validations import validateTimelines, validateConflation

CONTEXT = None
SCENARIO_LOADER = ScenarioLoader()
APPS = ['slowFixDecoderApp']

@pytest.fixture(autouse=True)
def setTestParameters(hostname, transactions, multithreaded, workspace, rundir):
  """
  A method run at the beginning of tests to set test context variables
  """

  from xpedite.util              import makeLogPath
  from xpedite.transport.net     import isIpLocal
  from xpedite.transport.remote  import Remote
  remote = None
  global CONTEXT # pylint: disable=global-statement
  if not isIpLocal(hostname):
    remote = Remote(hostname, makeLogPath('remote'))
    remote.__enter__()
  CONTEXT = Context(transactions, multithreaded, workspace)
  SCENARIO_LOADER.loadScenarios(rundir, APPS, [ScenarioType.Regular], remote)
  yield
  if remote:
    remote.__exit__(None, None, None)

def test_conflator():
  """
  Test logic to conflate profiles, timelines, timeline statistics, and routes
  """
  for scenarios in SCENARIO_LOADER:
    with scenarios as scenarios:
      report, _, _ = runXpediteRecord(CONTEXT, scenarios)
      validateConflation(scenarios, report.profiles)
      validateTimelines(scenarios, report.profiles, CONTEXT.txnCount)

def test_filter():
  """
  Test transaction filtering
  """
  from xpedite.analytics.timelineFilter import TimelineFilter
  txnId = 1
  for scenarios in SCENARIO_LOADER:
    with scenarios as scenarios:
      report, _, _ = runXpediteRecord(CONTEXT, scenarios)
      validateTimelines(scenarios, report.profiles, CONTEXT.txnCount)
      for profile in report.profiles:
        assert len(TimelineFilter(
          lambda txn: txn.txnId == txnId
      ).filterTimelines(profile.current.timelineCollection)) == txnId
