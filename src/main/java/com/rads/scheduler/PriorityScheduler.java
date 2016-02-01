package com.rads.scheduler;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import backtype.storm.event__init;
import backtype.storm.generated.Nimbus.Client;
import backtype.storm.generated.SupervisorSummary;
import backtype.storm.generated.TopologySummary;
import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.EvenScheduler;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.SchedulerAssignment;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

/**
 * This is a custom storm scheduler developed by rudraneel chakraborty
 */
public class PriorityScheduler implements IScheduler {

	private Map<Integer, ArrayList<String>> tpINFO;
	private Map<String, String> actualTPINFOFromCluster;
	private Map<String, String> actualSVINFOFromCluster;

	private long stTime = 0;
	private long eTime = 0;
	int sp=0;
	private Map<Integer, SupervisorDetails> svINFO = new HashMap<Integer, SupervisorDetails>();

	/*
	 * Holds the topology name and priority with key as topology
	 */
	private HashMap<String, Integer> TpToPriorityMap = new HashMap<String, Integer>();

	private DBHelper db = new DBHelper();

	public PriorityScheduler() {
		

	}

	public void prepare(Map conf) {
	}

	public void schedule(Topologies topologies, Cluster cluster) {

		try {
			/*
			 * TODO : DB Integration and logic change required
			 */

			//getImportantTP(topologies, cluster);
			
			initializeAtSP(topologies, cluster);
			this.stTime = System.currentTimeMillis();
			if (tpINFO != null) {

				Set<Integer> tpKeys = tpINFO.keySet();

				for (Integer p_int : tpKeys) {

					ArrayList<String> ptplist = tpINFO.get(p_int);


					for (String tpName : ptplist) {
						String impTP = tpName;
						TopologyDetails tdIMP = topologies.getByName(impTP);

						if (cluster.needsScheduling(tdIMP)) {

							// get the important topology

							slotDispute(topologies, cluster, tdIMP);
							System.out.println(" Topology " + tdIMP.getName() + " Need Scheduling ");
							int requestNumWorker = tdIMP.getNumWorkers();
							Map<String, List<ExecutorDetails>> needScheduling = cluster
									.getNeedsSchedulingComponentToExecutors(tdIMP);
							List<WorkerSlot> wsList = cluster.getAvailableSlots();
							int prioRityOfTP = this.TpToPriorityMap.get(tdIMP.getName());
							int numTPOnSamePriority = decideEQShare(topologies, cluster, prioRityOfTP);

							int totalFreeSlots = wsList.size();

							if (totalFreeSlots > 0) {

								int equalShareOfWorker = (totalFreeSlots / numTPOnSamePriority);
								if (equalShareOfWorker > requestNumWorker) {
									equalShareOfWorker = requestNumWorker;

								}

								if ((cluster.getAvailableSlots().size() > 0) && (equalShareOfWorker == 0)) {
									equalShareOfWorker = cluster.getAvailableSlots().size();

								}

								List<ExecutorDetails> execList = getEven(needScheduling);
								int numOfExecutors = execList.size();
								if (numOfExecutors > 0) {

									List<Integer> ExecutorDistToWorkers = getEvenAllocation(numOfExecutors,
											equalShareOfWorker);

									int counterToStart = 0;

									int rr = 1;
									for (int i = 0; i < ExecutorDistToWorkers.size(); i++) {

										WorkerSlot currentSlot = null;
										// wsList.get(i);

										Map<String, SupervisorDetails> svMap = cluster.getSupervisors();
										Set<String> keys = svMap.keySet();

										if (rr > this.svINFO.size()) {
											rr = 1;
										}
										for (String keyV : keys) {
											SupervisorDetails sd = this.svINFO.get(rr);
											try {
												if (cluster.getAvailableSlots(sd).size() > 0) {
													WorkerSlot wst = null;
													for (int q = 0; q < cluster.getAvailableSlots(sd).size(); q++) {

														WorkerSlot tmpwst = cluster.getAvailableSlots(sd).get(q);

														if (cluster.isSlotOccupied(tmpwst)) {

															continue;
														} else {
															wst = tmpwst;

															// break;
														}
														if (wst != null) {
															break;
														}
													}
													if (wst != null) {

														currentSlot = wst;

														rr++;
														break;
													} else {
														rr++;
													}

												}

												else {
													rr++;
												}
											} catch (Exception e) {

												System.out.println("Brutal for " + sd.getHost() + " " + e.getMessage());
											}
										}

										List<ExecutorDetails> chunkListOfExecutor = new ArrayList<ExecutorDetails>(
												execList.subList(counterToStart,
														counterToStart + ExecutorDistToWorkers.get(i)));

										try {

											cluster.assign(currentSlot, tdIMP.getId(), chunkListOfExecutor);
											counterToStart = counterToStart + ExecutorDistToWorkers.get(i);
											db.InsertSchedule(tdIMP.getName(), currentSlot.getNodeId(),
													currentSlot.getPort() + "");

										}

										catch (Exception e) {
											System.out.println(" Error assigning to slot " + currentSlot.getNodeId()
													+ " error: " + e.getMessage());

										}

									}

								}

							}

						}

					}
				}
			}
		}

		catch (Exception e)

		{
			System.out.println("Error on the main block " + e.getMessage());

		}

	
	this.eTime=System.currentTimeMillis();
	
	System.out.println("Total Time took in MS: "+(this.eTime-this.stTime));
	sp++;
	System.out.println("scheduling point "+sp);
	}

	public void slotDispute(Topologies topologies, Cluster cluster, TopologyDetails tdIMP) {

		boolean needRefactor1 = LowPriorityWorkerSlotDispute(topologies, cluster, tdIMP);
		boolean needRefactor2 = SamePriorityWorkerSlotDispute(topologies, cluster, tdIMP);

		if (needRefactor1 || needRefactor2) {
			//System.out.println("Slot dispute resolved. Rescheduling required");
			this.schedule(topologies, cluster);
		}
	}

	public ArrayList<Integer> getEvenAllocation(int num_tasks, int num_resources) {
		ArrayList<Integer> sc = new ArrayList<Integer>();
		if (num_tasks > num_resources) {

			int tmp = num_tasks;
			int bmp = num_resources;
			for (int i = 0; i < num_resources; i++) {

				int v = tmp / bmp;
				sc.add(v);

				tmp = tmp - v;
				bmp--;

			}
		} else {

			for (int i = 0; i < num_tasks; i++) {
				sc.add(1);
			}

		}

		Collections.sort(sc, Collections.reverseOrder());
		return sc;
	}

	private ArrayList<ExecutorDetails> getEven(Map<String, List<ExecutorDetails>> mp) {
		ArrayList<ExecutorDetails> olist = new ArrayList<ExecutorDetails>();

		Set<String> keys = mp.keySet();

		for (String ke : keys) {
			List<ExecutorDetails> nl = mp.get(ke);

			for (int i = 0; i < nl.size(); i++) {

				olist.add(nl.get(i));

			}

		}

		return olist;

	}

	private int decideEQShare(Topologies topologies, Cluster cluster, int pr) {

		int cnt = 0;
		ArrayList<String> al = tpINFO.get(pr);

		for (String tpName : al) {

			TopologyDetails td = topologies.getByName(tpName);
			int assignedWorker = cluster.getAssignedNumWorkers(td);

			if ((cluster.needsScheduling(td)) || ((cluster.getAvailableSlots().size() - assignedWorker) < 0)) {

				cnt++;
			}
		}

		return cnt;

	}

	public boolean LowPriorityWorkerSlotDispute(Topologies topologies, Cluster cluster, TopologyDetails tp) {
		boolean isAnySlotFlushed = false;
		int requestedNumWorker = tp.getNumWorkers();
		int assignedNumWorker = cluster.getAssignedNumWorkers(tp);
		int prioRityOfTP = this.TpToPriorityMap.get(tp.getName());
		int numTPOnSamePriority = decideEQShare(topologies, cluster, prioRityOfTP);
		// tpINFO.get(prioRityOfTP).size();
		int totalWorkerAvailable = cluster.getAvailableSlots().size() + cluster.getUsedSlots().size();
		if (totalWorkerAvailable > 0) {

			int equalShareOfWorker = (totalWorkerAvailable / numTPOnSamePriority);
			if (equalShareOfWorker > requestedNumWorker) {
				equalShareOfWorker = requestedNumWorker;

			}

			if ((equalShareOfWorker > cluster.getAvailableSlots().size()) && (assignedNumWorker < equalShareOfWorker)) {
				// We need to Free some ports from lower priority topologies

				int counter = 0;
				int numSlotRequired = cluster.getAvailableSlots().size() - equalShareOfWorker;

				Collection<WorkerSlot> wsl = cluster.getUsedSlots();

				for (WorkerSlot ws : wsl) {
					if (counter == numSlotRequired) {
						break;
					} else {
						String supervisorName = ws.getNodeId();
						String portID = ws.getPort() + "";
						String topologyName = db.checkSchedule(supervisorName, portID);

						if (topologyName.equals("none")) {

						} else {
							if ((this.TpToPriorityMap.get(tp.getName()) < this.TpToPriorityMap.get(topologyName))) {

								cluster.freeSlot(ws);
								deScheduleTP(topologies, cluster, topologies.getByName(topologyName));
								db.deleteSchedule(ws.getNodeId(), ws.getPort() + "");
								isAnySlotFlushed = true;
								counter++;
							}
						}

					}
				}

			} else {

			}
		} else {

		}
		return isAnySlotFlushed;
	}

	public boolean SamePriorityWorkerSlotDispute(Topologies topologies, Cluster cluster, TopologyDetails tp) {
		boolean isAnySlotFlushed = false;
		int requestedNumWorker = tp.getNumWorkers();
		int assignedNumWorker = cluster.getAssignedNumWorkers(tp);
		int prioRityOfTP = this.TpToPriorityMap.get(tp.getName());
		int numTPOnSamePriority = decideEQShare(topologies, cluster, prioRityOfTP);
		int totalWorkerAvailable = cluster.getAvailableSlots().size() + cluster.getUsedSlots().size();
		if (totalWorkerAvailable > 0) {

			int equalShareOfWorker = (totalWorkerAvailable / numTPOnSamePriority);

			if (equalShareOfWorker > requestedNumWorker) {
				equalShareOfWorker = requestedNumWorker;

			}

			
			
		if ((assignedNumWorker < equalShareOfWorker) && (cluster.getAvailableSlots().size() >= equalShareOfWorker)) {

				
				if (assignedNumWorker > 0) {
					SchedulerAssignment sa = cluster.getAssignmentById(tp.getId());
					Set<WorkerSlot> wsl1 = sa.getSlots();
					for (WorkerSlot ws : wsl1) {
						cluster.freeSlot(ws);
						System.out.println("Freed slot 1");
						db.deleteSchedule(ws.getNodeId(), ws.getPort() + "");
						isAnySlotFlushed = true;
					}

				}
			}
			
			
		else if ((assignedNumWorker < equalShareOfWorker) && (cluster.getAvailableSlots().size() < equalShareOfWorker)) {

				
				int counter = 0;
				int numSlotRequired = cluster.getAvailableSlots().size() - equalShareOfWorker;

				Collection<WorkerSlot> wsl = cluster.getUsedSlots();

				for (WorkerSlot ws : wsl) {
					if (counter == numSlotRequired) {
						break;
					} else {
						String supervisorName = ws.getNodeId();
						String portID = ws.getPort() + "";
						String topologyName = db.checkSchedule(supervisorName, portID);

						if (topologyName.equals("none")) {

						} else {
							if ((this.TpToPriorityMap.get(tp.getName()) == this.TpToPriorityMap.get(topologyName))) {

								// Is this topology consuming more worker
								// than required?
								TopologyDetails scheduledTP = topologies.getByName(topologyName);
								int scheduledTPAssignedWorker = cluster.getAssignedNumWorkers(scheduledTP);
								if (scheduledTPAssignedWorker > equalShareOfWorker) {

									cluster.freeSlot(ws);
									deScheduleTP(topologies, cluster, scheduledTP);
									db.deleteSchedule(ws.getNodeId(), ws.getPort() + "");
									isAnySlotFlushed = true;
									counter++;
								}
							}
						}

					}
				}

				if (assignedNumWorker > 0) {
					SchedulerAssignment sa = cluster.getAssignmentById(tp.getId());
					Set<WorkerSlot> wsl1 = sa.getSlots();
					for (WorkerSlot ws : wsl1) {
						cluster.freeSlot(ws);
						db.deleteSchedule(ws.getNodeId(), ws.getPort() + "");
						isAnySlotFlushed = true;
					}

				}
			}

			else {
				// ALL GOOD

			}
		}

		else {
			// Do Nothing; No worker slots available
		}

		return isAnySlotFlushed;
	}

	/*
	 * public boolean DoesMoreHigherPriorityTPExist(Topologies topologies,
	 * Cluster cluster, TopologyDetails tp) { boolean isExist = false; int
	 * ownTPPriority = this.TpToPriorityMap.get(tp.getName()); int
	 * numberofTPWithSamePriority = this.tpINFO.get(ownTPPriority).size();
	 * Set<String> tps = this.TpToPriorityMap.keySet();
	 * 
	 * for (String ks : tps) { if ((tp.getName()).equals(ks)) { continue; } else
	 * { if ((this.TpToPriorityMap.get(ks) <=
	 * this.TpToPriorityMap.get(tp.getName())) &&
	 * (cluster.needsScheduling(topologies.getByName(ks)))) { isExist = true; }
	 * }
	 * 
	 * }
	 * 
	 * return isExist;
	 * 
	 * }
	 */

	public void deScheduleTP(Topologies topologies, Cluster cluster, TopologyDetails tp) {

		try {
			SchedulerAssignment sa = cluster.getAssignmentById(tp.getId());

			Set<WorkerSlot> wsl = sa.getSlots();
			for (WorkerSlot ws : wsl) {

				cluster.freeSlot(ws);
				db.deleteSchedule(ws.getNodeId(), ws.getPort() + "");

			}
		}

		catch (Exception e) {

			System.out.println("Error descheduling " + e.getMessage());
		}
	}

	/*
	 * Function to get the topology information from database. TODO: Require DB
	 * integration
	 */

	public void getActualValuesFromCluster(Topologies topology, Cluster cluster) {
		try {
			this.actualSVINFOFromCluster = new HashMap<String, String>();
			this.actualTPINFOFromCluster = new HashMap<String, String>();
			Map conf = Utils.readStormConfig();
			Client client = NimbusClient.getConfiguredClient(conf).getClient();

			List<TopologySummary> topologyList = client.getClusterInfo().get_topologies();

			for (TopologySummary tps : topologyList) {
				this.actualTPINFOFromCluster.put(tps.get_name(), tps.get_status());

			}
			List<SupervisorSummary> svList = client.getClusterInfo().get_supervisors();

			for (SupervisorSummary tps : svList) {
				this.actualSVINFOFromCluster.put(tps.get_host(), tps.get_num_workers() + "");

			}
		}

		catch (Exception e) {

		}

	}

	public void getImportantTP(Topologies topologies, Cluster cluster) {

	}

	public void initializeAtSP(Topologies topologies, Cluster cluster) {
		Date date = new Date();
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		String dt = dateFormat.format(date);
		System.out.println(" Starting Scheduling @" + dt);
		getActualValuesFromCluster(topologies, cluster);

		// Topologies
		HashMap<Integer, ArrayList<String>> mp = new HashMap<Integer, ArrayList<String>>();
		ArrayList<PriorityRegister> prList = db.getPriorityRegister();
		ArrayList<String> plist = new ArrayList<String>();
		for (PriorityRegister pr : prList) {
			String tpName = pr.getTpname();
			int priorityTP = 0;
			if (pr.isActive()) {
				priorityTP = pr.getPriority();
			} else {
				priorityTP = pr.getDefaultpriority();
			}

			if (this.actualTPINFOFromCluster.containsKey(tpName)) {
				plist.add(tpName);
				TpToPriorityMap.put(tpName, priorityTP);
			}

		}
		for (String tpname : plist) {

			if (TpToPriorityMap.containsKey(tpname)) {

				int prt = TpToPriorityMap.get(tpname);

				if (mp.containsKey(prt)) {

					mp.get(prt).add(tpname);
				}

				else {
					ArrayList<String> nl = new ArrayList<String>();
					nl.add(tpname);
					mp.put(prt, nl);

				}
			}

		}

		Map<Integer, ArrayList<String>> smp = new TreeMap<Integer, ArrayList<String>>(mp);
		this.tpINFO = smp;

		// Supervisors

		Map<String, SupervisorDetails> sv = cluster.getSupervisors();
		Set<String> keyS = sv.keySet();
		int svcn = 1;
		for (String keySV : keyS) {

			if (this.actualSVINFOFromCluster.containsKey(sv.get(keySV).getHost())) {
				this.svINFO.put(svcn, sv.get(keySV));
			     svcn++;
			}

		}

		System.out.println("Number of topologies to be scheduled " + this.TpToPriorityMap.size());
		System.out.println("Number of Supervisors available " + this.svINFO.size());
		System.out.println("Total Worker process available " + cluster.getAvailableSlots().size());

	}

}
