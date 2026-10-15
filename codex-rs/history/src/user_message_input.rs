//! Converts stored model content without exposing harness-added media labels as user text.

use codex_protocol::models::ContentItem;
use codex_protocol::models::is_audio_close_tag_text;
use codex_protocol::models::is_audio_open_tag_text;
use codex_protocol::models::is_image_close_tag_text;
use codex_protocol::models::is_image_open_tag_text;
use codex_protocol::models::is_local_audio_close_tag_text;
use codex_protocol::models::is_local_audio_open_tag_text;
use codex_protocol::models::is_local_image_close_tag_text;
use codex_protocol::models::is_local_image_open_tag_text;
use codex_protocol::user_input::UserInput;

/// Reconstructs user input after the caller has excluded contextual messages.
/// Media labels are omitted only when adjacent to the corresponding media item.
pub fn user_message_input(message: &[ContentItem]) -> Vec<UserInput> {
    let mut content: Vec<UserInput> = Vec::new();

    for (idx, content_item) in message.iter().enumerate() {
        match content_item {
            ContentItem::InputText { text } => {
                let is_image_label = ((is_local_image_open_tag_text(text)
                    || is_image_open_tag_text(text))
                    && matches!(message.get(idx + 1), Some(ContentItem::InputImage { .. })))
                    || (idx > 0
                        && (is_local_image_close_tag_text(text) || is_image_close_tag_text(text))
                        && matches!(message.get(idx - 1), Some(ContentItem::InputImage { .. })));
                let is_audio_label = ((is_local_audio_open_tag_text(text)
                    || is_audio_open_tag_text(text))
                    && matches!(message.get(idx + 1), Some(ContentItem::InputAudio { .. })))
                    || (idx > 0
                        && (is_local_audio_close_tag_text(text) || is_audio_close_tag_text(text))
                        && matches!(message.get(idx - 1), Some(ContentItem::InputAudio { .. })));
                if is_image_label || is_audio_label {
                    continue;
                }
                content.push(UserInput::Text {
                    text: text.clone(),
                    // Model input content does not carry UI element ranges.
                    text_elements: Vec::new(),
                });
            }
            ContentItem::InputImage { image, detail } => {
                content.push(UserInput::Image {
                    image: image.clone(),
                    detail: *detail,
                });
            }
            ContentItem::InputAudio { audio_url } => {
                content.push(UserInput::Audio {
                    audio_url: audio_url.clone(),
                });
            }
            ContentItem::OutputText { .. } => {}
        }
    }

    content
}

#[cfg(test)]
#[path = "user_message_input_tests.rs"]
mod tests;
