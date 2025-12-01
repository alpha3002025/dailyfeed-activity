package click.dailyfeed.activity.monitor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class MemoryMonitor {

	private static final double MB = 1024.0 * 1024.0;

	@EventListener(ApplicationReadyEvent.class)
	public void onApplicationReady() {
		logMemoryUsage("Application startup completed");
	}

	@Scheduled(fixedRate = 60000) // Every 1 minute
	public void logMemoryUsagePeriodically() {
		logMemoryUsage("Periodic memory check");
	}

	private void logMemoryUsage(String context) {
		Runtime runtime = Runtime.getRuntime();
		long maxMemory = runtime.maxMemory();
		long totalMemory = runtime.totalMemory();
		long freeMemory = runtime.freeMemory();
		long usedMemory = totalMemory - freeMemory;

		double maxMemoryMB = maxMemory / MB;
		double totalMemoryMB = totalMemory / MB;
		double usedMemoryMB = usedMemory / MB;
		double freeMemoryMB = freeMemory / MB;
		double usagePercentage = (usedMemory * 100.0) / maxMemory;

		log.info("=== Memory Usage: {} ===", context);
		log.info("Max Memory (JVM):   {}/{} MB ({} Mi)",
			String.format("%.2f", maxMemoryMB),
			String.format("%.2f", maxMemoryMB / 1.024),
			String.format("%.0f", maxMemoryMB / 1.024));
		log.info("Used Memory:        {} MB ({} Mi) - {}/{} %",
			String.format("%.2f", usedMemoryMB),
			String.format("%.0f", usedMemoryMB / 1.024),
			String.format("%.2f", usagePercentage),
			"100.00");
		log.info("Free Memory:        {} MB ({} Mi)",
			String.format("%.2f", freeMemoryMB),
			String.format("%.0f", freeMemoryMB / 1.024));
		log.info("Total Allocated:    {} MB ({} Mi)",
			String.format("%.2f", totalMemoryMB),
			String.format("%.0f", totalMemoryMB / 1.024));
		log.info("K8s Memory Request: 512Mi, Limit: 800Mi");
		log.info("HPA Threshold:      {} Mi (80% of 512Mi)",
			String.format("%.0f", 512 * 0.8));
	}
}