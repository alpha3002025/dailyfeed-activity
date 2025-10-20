package click.dailyfeed.activity.domain.member.activity.service;

import click.dailyfeed.activity.domain.member.activity.document.MemberActivityDocument;
import click.dailyfeed.activity.domain.member.activity.mapper.MemberActivityMapper;
import click.dailyfeed.activity.domain.member.activity.repository.mongo.MemberActivityMongoRepository;
import click.dailyfeed.code.domain.activity.dto.MemberActivityDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
@Transactional
@RequiredArgsConstructor
public class MemberActivityService {
    private final MemberActivityMongoRepository memberActivityMongoRepository;
    private final MemberActivityMapper memberActivityMapper;

    public MemberActivityDto.MemberActivity createPostsMemberActivity(MemberActivityDto.PostActivityRequest postActivityRequest) {
        MemberActivityDocument document = memberActivityMapper.fromPostRequest(postActivityRequest);
        MemberActivityDocument savedDocument = memberActivityMongoRepository.save(document);
        return memberActivityMapper.fromDocument(savedDocument);
    }

    public MemberActivityDto.MemberActivity createCommentsMemberActivity(MemberActivityDto.CommentActivityRequest commentActivityRequest) {
        MemberActivityDocument document = memberActivityMapper.fromCommentRequest(commentActivityRequest);
        MemberActivityDocument savedDocument = memberActivityMongoRepository.save(document);
        return memberActivityMapper.fromDocument(savedDocument);
    }

    public MemberActivityDto.MemberActivity createPostLikeActivity(MemberActivityDto.PostLikeActivityRequest postLikeActivityRequest) {
        MemberActivityDocument document = memberActivityMapper.fromPostLikeRequest(postLikeActivityRequest);
        MemberActivityDocument savedDocument = memberActivityMongoRepository.save(document);
        return memberActivityMapper.fromDocument(savedDocument);
    }

    public MemberActivityDto.MemberActivity createCommentLikeActivity(MemberActivityDto.CommentLikeActivityRequest commentLikeActivityRequest) {
        MemberActivityDocument document = memberActivityMapper.fromCommentLikeRequest(commentLikeActivityRequest);
        MemberActivityDocument savedDocument = memberActivityMongoRepository.save(document);
        return memberActivityMapper.fromDocument(savedDocument);
    }

}
