package com.rads.scheduler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import backtype.storm.event__init;
import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.EvenScheduler;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.SchedulerAssignment;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;

/**
 * This is a custom storm scheduler developed by rudraneel chakraborty
 */
public class PriorityScheduler2cp implements IScheduler {

	/*
	 * Holds the schedule of the components of a topology to different worker
	 * slots of a supervisor node
	 */
	private HashMap<String, String> scheduleMap = new HashMap<String, String>();

	private Map<Integer, ArrayList<String>> tpINFO;
	/*
	 * Holds the topology name and priority with key as topology
	 */
	private HashMap<String, Integer> TpToPriorityMap = new HashMap<String, Integer>();

	private DBHelper db = new DBHelper();

	public void prepare(Map conf) {
	}

	public void schedule(Topologies topologies, Cluster cluster) {

		int totalWorkerAvailable = cluster.getAvailableSlots().size() + cluster.getUsedSlots().size();
		System.out.println("Started scheduling" + " total slots availavle " + cluster.getAvailableSlots().size()
				+ " occupied " + cluster.getUsedSlots().size());

		try {
			/*
			 * TODO : DB Integration and logic change required
			 */

			getImportantTP(topologies);
			if (tpINFO != null) {

				Set<Integer> tpKeys = tpINFO.keySet();

				for (Integer p_int : tpKeys) {

					ArrayList<String> ptplist = tpINFO.get(p_int);

					int evn_rs_dist = ptplist.size();

					System.out.println(" Number of tp in pgroup " + evn_rs_dist);
					for (String tpName : ptplist) {

						String impTP = tpName;
						// get the important topology
						TopologyDetails tdIMP = topologies.getByName(impTP);

						slotDispute(topologies, cluster, tdIMP);

						if (cluster.needsScheduling(tdIMP)) {

							System.out.println(" Topology in hand " + impTP);
							/*
							 * Requested Number of worker slots by the topology
							 * Divided by the resource distribution factor. This
							 * is essential for all the topologies with similar
							 * priority to share the resource
							 */
							int requestedWorker = (tdIMP.getNumWorkers() / evn_rs_dist);
							// At least one worker process will be required
							if (requestedWorker < 1) {
								requestedWorker = 1;
							}

							System.out.println("Topology worker request " + requestedWorker);

							// Find out the need scheduling components of this
							// topology
							Map<String, List<ExecutorDetails>> unevencomponentToExecutors = cluster
									.getNeedsSchedulingComponentToExecutors(tdIMP);

							System.out.println("TP " + tdIMP.getName()
									+ " need scheduling with number of components to be scheduled "
									+ unevencomponentToExecutors.keySet());

							// Get all the supervisors
							Collection<SupervisorDetails> svList = cluster.getSupervisors().values();
							ArrayList<SupervisorDetails> supvList = new ArrayList<SupervisorDetails>();
							supvList.addAll(svList);

							/*
							 * The executors in components are not necessarily
							 * even. So we manipulate the map above and
							 * distribute the executors evenly
							 */

							Map<String, List<ExecutorDetails>> componentToExecutors = divideEqually(
									getEven(unevencomponentToExecutors), requestedWorker);

							// Get the keys
							Set<String> execKeys = componentToExecutors.keySet();
							ArrayList<String> compList = new ArrayList<String>();
							// To ensure FIFO , take it to a list
							for (String keyS : execKeys) {
								compList.add(keyS);
							}

							int number_of_components = compList.size();

							int number_of_supervisors = svList.size();
							// Decide how to spread the load among supervisors
							// evenly
							ArrayList<Integer> allocateComponentToSupervisor = getEvenAllocation(number_of_components,
									number_of_supervisors);
							// Decide how to spread worker processes among
							// supervisor
							ArrayList<Integer> allocateWorkerToSupervisor = getEvenAllocation(requestedWorker,
									number_of_supervisors);
							// The components should be evenly spread out in
							// supervisors

							System.out
									.println(" Started Scheduling with following stats. \nRequested Number of workers: "
											+ tdIMP.getNumWorkers() + "\nTopology Components " + compList.toString()
											+ "\nNumber of Supervisors " + svList.size());
							for (int i = 0; i < supvList.size(); i++) {

								// Get the supervisor
								SupervisorDetails csv = supvList.get(i);
								/*
								 * Get the number of even share of tasks for
								 * this supervisor
								 */
								int num_com_tobe_scheduled = allocateComponentToSupervisor.get(i);
								int num_slots_need_fromsv = allocateWorkerToSupervisor.get(i);
								/*
								 * Check if required number of worker is
								 * available for this supervisor
								 */
								List<WorkerSlot> availableSlots = cluster.getAvailableSlots(csv);
								/*
								 * Check if the total number of lots (available
								 * +used) is less than requested number of slots
								 */

								if (num_slots_need_fromsv > (cluster.getAvailableSlots(csv).size()
										+ cluster.getUsedSlots().size())) {
									/*
									 * Change the required number of slots to
									 * the slot upper bound of this supervisor
									 */
									num_slots_need_fromsv = cluster.getAvailableSlots(csv).size()
											+ cluster.getUsedSlots().size();

								}

								/*
								 * The requested number of slot is not
								 * available. We need to free some resource.
								 * TODO : Blindly freeing up slots. Will install
								 * some logic here for priority considerations
								 */
								
								
//								if (num_slots_need_fromsv > availableSlots.size()) {
//									for (Integer port : cluster.getUsedPorts(csv)) {
//
//										/*
//										 * Need to decide whether a high
//										 * priority topology is occupying this
//										 * slot. In that case, the port will not
//										 * be flushed
//										 */
//
//										WorkerSlot ws = new WorkerSlot(csv.getId(), port);
//										String tpOnSlot = scheduleMap.get(ws.getNodeId() + "##" + ws.getPort());
//
//										if (tpOnSlot.equals(impTP)) {
//											/*
//											 * Do nothing. Its essentially the
//											 * same topology
//											 */
//										} else if (TpToPriorityMap.get(tpOnSlot) >= (TpToPriorityMap.get(impTP))) {
//											/*
//											 * Same priority Topologies. This
//											 * situation is already taken care
//											 * of by splitting the resource
//											 * requirement
//											 */
//										}
//
//										else if (TpToPriorityMap.get(tpOnSlot) < (TpToPriorityMap.get(impTP))) {
//											cluster.freeSlot(new WorkerSlot(csv.getId(), port));
//
//										} else {
//
//											// This should never be the case
//
//											System.out.println(" Unexpecte logic situation. Require debugging");
//										}
//
//									}
//									System.out.println(
//											"No slots were available, so preempted tasks and some ports are free ");
//								}

								// Again Get the free slots
								availableSlots = cluster.getAvailableSlots(csv);
								/*
								 * Get an even schedule for the components to
								 * worker slots
								 */
								if (availableSlots.size() > 0) {

									ArrayList<Integer> even_comp_to_workers = getEvenAllocation(num_com_tobe_scheduled,
											availableSlots.size());

									for (int k = 0; k < even_comp_to_workers.size(); k++) {

										int num_of_component = even_comp_to_workers.get(k);

										System.out.println(" For TP "+tdIMP.getName() +" Scheduling on "+csv.getHost() + "with num of components "+num_of_component +" in worker "+even_comp_to_workers.size());
										
										for (int x = 0; x < num_of_component; x++) {
											List<ExecutorDetails> executors = componentToExecutors.get(compList.get(x));

											// Schedule the component to a
											// worker
											// slot
											try {
												cluster.assign(availableSlots.get(k), tdIMP.getId(), executors);

												db.InsertSchedule(tdIMP.getName(), availableSlots.get(k).getNodeId(), availableSlots.get(k).getPort()+"");
												System.out.println(" Scheduled component " + compList.get(x)
														+ "on supervisor " + csv.getId());

												scheduleMap.put(availableSlots.get(k).getNodeId() + "##"
														+ availableSlots.get(k).getPort(), impTP);
												// remove from FIFO
												compList.remove(x);
												//System.out.println(" Removed this component from the queue "
												//		+ executors.toString());

											}

											catch (Exception e) {
												// Something is very wrong
												// !!!!!!
												System.out.println("Caught on the schedule block " + e.getMessage());

											}

										}

									}

								}

							}

							System.out.println("Allocation of the topology "
									+ cluster.getAssignmentById(tdIMP.getId()).getExecutorToSlot());
							System.out
									.println("Does it need scheduling anymore ?? : " + cluster.needsScheduling(tdIMP));
							new EvenScheduler().schedule(topologies, cluster);

						} else {

							System.out.println("Topology " + impTP + " does not need scheduling");
						}

					}

				}

			}
		}

		catch (

		Exception e)

		{

			System.out.println("Exception message " + e.getMessage());
		}

	}

	public void slotDispute(Topologies topologies, Cluster cluster, TopologyDetails tdIMP) {

		boolean needRefactor1 = LowPriorityWorkerSlotDispute(topologies, cluster, tdIMP);
		boolean needRefactor2 = SamePriorityWorkerSlotDispute(topologies, cluster, tdIMP);

		if (needRefactor1 || needRefactor2) {
			System.out.println("Slot dispute resolved. Rescheduling required");
			schedule(topologies, cluster);
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
		int count = 0;
		ArrayList<ExecutorDetails> olist = new ArrayList<ExecutorDetails>();

		Set<String> keys = mp.keySet();

		for (String ke : keys) {
			List<ExecutorDetails> nl = mp.get(ke);

			for (int i = 0; i < nl.size(); i++) {

				olist.add(nl.get(i));

			}

		}

		Collections.shuffle(olist);
		return olist;

	}

	private Map<String, List<ExecutorDetails>> divideEqually(List<ExecutorDetails> al, int divisionTag) {

		int counter = 0;
		Map<String, List<ExecutorDetails>> nmp = new HashMap<String, List<ExecutorDetails>>();

		List<Integer> dtag = getEvenAllocation(al.size(), divisionTag);
		// System.out.println(dtag.toString());
		// System.out.println(al.toString());
		for (int i = 0; i < dtag.size(); i++) {
			String keyS = "Key-" + i;

			List<ExecutorDetails> nl = new ArrayList<ExecutorDetails>(al.subList(counter, counter + (dtag.get(i))));
			nmp.put(keyS, nl);

			counter = counter + dtag.get(i);

		}

		return nmp;

	}

	public boolean LowPriorityWorkerSlotDispute(Topologies topologies, Cluster cluster, TopologyDetails tp) {
		boolean isAnySlotFlushed = false;
		int requestedNumWorker = tp.getNumWorkers();
		int assignedNumWorker = cluster.getAssignedNumWorkers(tp);
		int prioRityOfTP = this.TpToPriorityMap.get(tp.getName());
		int numTPOnSamePriority = tpINFO.get(prioRityOfTP).size();
		int totalWorkerAvailable = cluster.getAvailableSlots().size() + cluster.getUsedSlots().size();
		if (totalWorkerAvailable > 0) {

			int equalShareOfWorker = (totalWorkerAvailable / numTPOnSamePriority);
			if (equalShareOfWorker > requestedNumWorker) {
				equalShareOfWorker = requestedNumWorker;

			}

			if ((equalShareOfWorker < cluster.getAvailableSlots().size()) && (assignedNumWorker < equalShareOfWorker)) {
				// We need to Free some ports from lower priority topologies

				Collection<SupervisorDetails> svList = cluster.getSupervisors().values();
				int counter = 0;
				int numSlotRequired = cluster.getAvailableSlots().size() - equalShareOfWorker;
				for (SupervisorDetails sd : svList) {

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
								if ((this.TpToPriorityMap.get(tp.getName()) > this.TpToPriorityMap.get(topologyName))) {

									cluster.freeSlot(ws);
									isAnySlotFlushed = true;
									counter++;
								}
							}

						}
					}

				}

			}
		} else {
			// Do Nothing. NO worker Process available
		}
		return isAnySlotFlushed;
	}

	public boolean SamePriorityWorkerSlotDispute(Topologies topologies, Cluster cluster, TopologyDetails tp) {
		boolean isAnySlotFlushed = false;
		int requestedNumWorker = tp.getNumWorkers();
		int assignedNumWorker = cluster.getAssignedNumWorkers(tp);
		int prioRityOfTP = this.TpToPriorityMap.get(tp.getName());
		int numTPOnSamePriority = tpINFO.get(prioRityOfTP).size();
		int totalWorkerAvailable = cluster.getAvailableSlots().size() + cluster.getUsedSlots().size();
		if (totalWorkerAvailable > 0) {

			int equalShareOfWorker = (totalWorkerAvailable / numTPOnSamePriority);

			if (equalShareOfWorker > requestedNumWorker) {
				equalShareOfWorker = requestedNumWorker;

			}
			// Topology is consuming more resource
			if (assignedNumWorker > equalShareOfWorker) {
				int numWorkerSlotToRelease = assignedNumWorker - equalShareOfWorker;
				SchedulerAssignment sa = cluster.getAssignmentById(tp.getId());

				Set<WorkerSlot> wsl = sa.getSlots();
				int counter = 0;
				for (WorkerSlot ws : wsl) {
					if (counter == numWorkerSlotToRelease) {
						break;
					} else {
						cluster.freeSlot(ws);
						isAnySlotFlushed = true;
						counter++;
					}
				}

			}

			// Topology is consuming less resource
			else if ((assignedNumWorker < equalShareOfWorker) && (totalWorkerAvailable >= equalShareOfWorker)) {

				Collection<SupervisorDetails> svList = cluster.getSupervisors().values();
				int counter = 0;
				int numSlotRequired = cluster.getAvailableSlots().size() - equalShareOfWorker;
				for (SupervisorDetails sd : svList) {

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
								if ((this.TpToPriorityMap.get(tp.getName()) == this.TpToPriorityMap
										.get(topologyName))) {

									// Is this topology consuming more worker
									// than required?
									TopologyDetails scheduledTP = topologies.getByName(topologyName);
									int scheduledTPAssignedWorker = cluster.getAssignedNumWorkers(scheduledTP);
									if (scheduledTPAssignedWorker > equalShareOfWorker) {

										cluster.freeSlot(ws);
										isAnySlotFlushed = true;
										counter++;
									}
								}
							}

						}
					}

				}

				if (assignedNumWorker > 0) {
					SchedulerAssignment sa = cluster.getAssignmentById(tp.getId());
					// We should remove the scheduling for this topology so that
					// it's
					// components can be redistributed
					Set<WorkerSlot> wsl = sa.getSlots();
					for (WorkerSlot ws : wsl) {
						cluster.freeSlot(ws);
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

	public int checkAndFreeWorkerSlots(Cluster cluster, Collection<SupervisorDetails> svList) {

		SchedulerAssignment sa = cluster.getAssignmentById("j");

		return 0;
	}

	/*
	 * Function to get the topology information from database. TODO: Require DB
	 * integration
	 */

	public void getImportantTP(Topologies topologies) {

		HashMap<Integer, ArrayList<String>> mp = new HashMap<Integer, ArrayList<String>>();
		int priority = 1;

		ArrayList<String> plist = new ArrayList<String>();
		if (topologies.getByName("accesslogtp") != null) {

			plist.add("accesslogtp");
			TpToPriorityMap.put("accesslogtp", 1);
		}

		if (topologies.getByName("errorlogtp") != null) {
			plist.add("errorlogtp");
			TpToPriorityMap.put("errorlogtp", 1);

		}
		mp.put(1, plist);
		Map<Integer, ArrayList<String>> smp = new TreeMap<Integer, ArrayList<String>>(mp);

		this.tpINFO = smp;

	}

}
