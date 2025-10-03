# post_activities
- 제거
- timeline 내에서 memberId 의 팔로잉 리스트(최대 1000개)에 해당하는 post 들 중 최근 n건을 가져오는 방식으로 변환

# member_activities
- 글 작성, 글 수정, 글 삭제 : content-svc 에서 member-activity-yyyyMMdd 로 이벤트 발행
- 글 조회, 댓글 조회 : timeline-svc 에서 member-activity-yyyyMMdd 로 이벤트 발행
- 댓글 작성, 댓글 수정, 댓글 삭제 : content-svc 에서 member-activity-yyyyMMdd 로 이벤트 발행

# activity-svc
- member-activity-yyyyMMdd 를 listen
- MemberActivityEvent → `{MemberActivityDocument | newPost, newComment, newMember}`


# 그럼 이제 어떻게 할 것인지?
timeline-svc 를 먼저 수정
- (1) 팔로잉 멤버 리스트 조회
- (2) (1)의 멤버 리스트에 해당하는 최근 글들 조회

activity-svc 에 리스너 작성
- timeline-svc 에 작성해둔 Kafka Listener 활용

timeline-svc, content-svc
- content-svc 에서 사용 중인 메시지 발송 코드 활용

