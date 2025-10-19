package click.dailyfeed.activity;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.mongodb.config.EnableMongoAuditing;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@EnableScheduling
@EnableMongoAuditing
@EnableMongoRepositories(
		basePackages = "click.dailyfeed.activity.domain.**.repository.mongo",
		mongoTemplateRef = "mongoTemplate"
)
@EnableTransactionManagement
@SpringBootApplication
@ComponentScan(basePackages = {
		"click.dailyfeed.activity",
		"click.dailyfeed.pvc",
		"click.dailyfeed.redis",
		"click.dailyfeed.kafka",
		"click.dailyfeed.pagination",
})
public class ActivityApplication {

	public static void main(String[] args) {
		SpringApplication.run(ActivityApplication.class, args);
	}

}
