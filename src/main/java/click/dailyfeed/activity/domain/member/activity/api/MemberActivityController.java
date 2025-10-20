package click.dailyfeed.activity.domain.member.activity.api;

import click.dailyfeed.code.domain.activity.dto.MemberActivityDto;
import click.dailyfeed.activity.domain.member.activity.service.MemberActivityService;
import click.dailyfeed.code.domain.member.member.dto.MemberProfileDto;
import click.dailyfeed.code.global.web.code.ResponseSuccessCode;
import click.dailyfeed.code.global.web.response.DailyfeedServerResponse;
import click.dailyfeed.feign.config.web.annotation.AuthenticatedMemberProfileSummary;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/member-activities")
@RequiredArgsConstructor
public class MemberActivityController {
    private final MemberActivityService memberActivityService;

    @PostMapping("/posts")
    public DailyfeedServerResponse<MemberActivityDto.MemberActivity> createPostsMemberActivity(
            @AuthenticatedMemberProfileSummary MemberProfileDto.Summary member,
            @RequestHeader(value = "Authorization", required = false) String token,
            @RequestBody MemberActivityDto.PostActivityRequest postActivityRequest ){

        MemberActivityDto.MemberActivity result = memberActivityService.createPostsMemberActivity(postActivityRequest);
        return DailyfeedServerResponse.<MemberActivityDto.MemberActivity>builder()
                .status(HttpStatus.CREATED.value())
                .data(result)
                .result(ResponseSuccessCode.SUCCESS)
                .build();
    }

    @PostMapping("/comments")
    public DailyfeedServerResponse<MemberActivityDto.MemberActivity> createCommentsMemberActivity(
            @AuthenticatedMemberProfileSummary MemberProfileDto.Summary member,
            @RequestHeader(value = "Authorization", required = false) String token,
            @RequestBody MemberActivityDto.CommentActivityRequest commentActivityRequest ){

        MemberActivityDto.MemberActivity result = memberActivityService.createCommentsMemberActivity(commentActivityRequest);
        return DailyfeedServerResponse.<MemberActivityDto.MemberActivity>builder()
                .status(HttpStatus.CREATED.value())
                .data(result)
                .result(ResponseSuccessCode.SUCCESS)
                .build();
    }

    @PostMapping("/posts/likes")
    public DailyfeedServerResponse<MemberActivityDto.MemberActivity> createPostLikeActivity(
            @AuthenticatedMemberProfileSummary MemberProfileDto.Summary member,
            @RequestHeader(value = "Authorization", required = false) String token,
            @RequestBody MemberActivityDto.PostLikeActivityRequest postLikeActivityRequest ){

        MemberActivityDto.MemberActivity result = memberActivityService.createPostLikeActivity(postLikeActivityRequest);
        return DailyfeedServerResponse.<MemberActivityDto.MemberActivity>builder()
                .status(HttpStatus.CREATED.value())
                .data(result)
                .result(ResponseSuccessCode.SUCCESS)
                .build();
    }

    @PostMapping("/comments/likes")
    public DailyfeedServerResponse<MemberActivityDto.MemberActivity> createCommentLikeActivity(
            @AuthenticatedMemberProfileSummary MemberProfileDto.Summary member,
            @RequestHeader(value = "Authorization", required = false) String token,
            @RequestBody MemberActivityDto.CommentLikeActivityRequest commentLikeActivityRequest ){

        MemberActivityDto.MemberActivity result = memberActivityService.createCommentLikeActivity(commentLikeActivityRequest);
        return DailyfeedServerResponse.<MemberActivityDto.MemberActivity>builder()
                .status(HttpStatus.CREATED.value())
                .data(result)
                .result(ResponseSuccessCode.SUCCESS)
                .build();
    }
}
