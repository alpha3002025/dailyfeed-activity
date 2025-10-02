package click.dailyfeed.activity;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
@ComponentScan(basePackages = {
		"click.dailyfeed.activity",
		"click.dailyfeed.redis",
		"click.dailyfeed.kafka",
})
public class ActivityApplication {

	public static void main(String[] args) {
		SpringApplication.run(ActivityApplication.class, args);
	}

}
