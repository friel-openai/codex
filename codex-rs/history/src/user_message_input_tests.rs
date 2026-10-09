use codex_protocol::items::UserMessageItem;
use codex_protocol::models::ContentItem;
use codex_protocol::models::DEFAULT_IMAGE_DETAIL;
use codex_protocol::models::ImageReference;
use codex_protocol::user_input::UserInput;
use pretty_assertions::assert_eq;

use super::user_message_input;

#[test]
fn media_labels_and_output_text_do_not_change_recovered_user_text() {
    for (image_label, audio_label) in [
        ("<image>", "<audio>"),
        (
            r#"<image name=[Image #1] path="/tmp/local.png">"#,
            r#"<audio name=[Audio #1] path="/tmp/local.wav">"#,
        ),
    ] {
        let image = ImageReference::File {
            file_id: "file_123".to_string(),
        };
        let audio_url = "data:audio/wav;base64,abc".to_string();
        let content = vec![
            ContentItem::InputText {
                text: "first".to_string(),
            },
            ContentItem::InputText {
                text: image_label.to_string(),
            },
            ContentItem::InputImage {
                image: image.clone(),
                detail: Some(DEFAULT_IMAGE_DETAIL),
            },
            ContentItem::InputText {
                text: "</image>".to_string(),
            },
            ContentItem::OutputText {
                text: "not user input".to_string(),
            },
            ContentItem::InputText {
                text: audio_label.to_string(),
            },
            ContentItem::InputAudio {
                audio_url: audio_url.clone(),
            },
            ContentItem::InputText {
                text: "</audio>".to_string(),
            },
            ContentItem::InputText {
                text: "second".to_string(),
            },
        ];

        let input = user_message_input(&content);

        assert_eq!(
            input,
            vec![
                UserInput::Text {
                    text: "first".to_string(),
                    text_elements: Vec::new(),
                },
                UserInput::Image {
                    image,
                    detail: Some(DEFAULT_IMAGE_DETAIL),
                },
                UserInput::Audio { audio_url },
                UserInput::Text {
                    text: "second".to_string(),
                    text_elements: Vec::new(),
                },
            ]
        );
        assert_eq!(UserMessageItem::new(&input).message(), "firstsecond");
    }
}

#[test]
fn labels_without_matching_adjacent_media_remain_user_text() {
    let audio_url = "data:audio/wav;base64,abc".to_string();
    let content = vec![
        ContentItem::InputText {
            text: "<audio>".to_string(),
        },
        ContentItem::InputText {
            text: "<image>".to_string(),
        },
        ContentItem::InputAudio {
            audio_url: audio_url.clone(),
        },
        ContentItem::InputText {
            text: "</image>".to_string(),
        },
        ContentItem::InputText {
            text: "</audio>".to_string(),
        },
    ];

    let input = user_message_input(&content);

    assert_eq!(
        input,
        vec![
            UserInput::Text {
                text: "<audio>".to_string(),
                text_elements: Vec::new(),
            },
            UserInput::Text {
                text: "<image>".to_string(),
                text_elements: Vec::new(),
            },
            UserInput::Audio { audio_url },
            UserInput::Text {
                text: "</image>".to_string(),
                text_elements: Vec::new(),
            },
            UserInput::Text {
                text: "</audio>".to_string(),
                text_elements: Vec::new(),
            },
        ]
    );
    assert_eq!(
        UserMessageItem::new(&input).message(),
        "<audio><image></image></audio>"
    );
}
