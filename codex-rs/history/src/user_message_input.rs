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

/// User-input parts with their original indices. Media wrapper labels are not
/// user text; retain the indices so history edits can carry the right annotations.
pub fn user_input_parts(message: &[ContentItem]) -> impl Iterator<Item = (usize, &ContentItem)> {
    message
        .iter()
        .enumerate()
        .filter(|(idx, content_item)| match content_item {
            ContentItem::InputText { text } => {
                let is_image_label = ((is_local_image_open_tag_text(text)
                    || is_image_open_tag_text(text))
                    && matches!(message.get(idx + 1), Some(ContentItem::InputImage { .. })))
                    || (*idx > 0
                        && (is_local_image_close_tag_text(text) || is_image_close_tag_text(text))
                        && matches!(message.get(idx - 1), Some(ContentItem::InputImage { .. })));
                let is_audio_label = ((is_local_audio_open_tag_text(text)
                    || is_audio_open_tag_text(text))
                    && matches!(message.get(idx + 1), Some(ContentItem::InputAudio { .. })))
                    || (*idx > 0
                        && (is_local_audio_close_tag_text(text) || is_audio_close_tag_text(text))
                        && matches!(message.get(idx - 1), Some(ContentItem::InputAudio { .. })));
                !is_image_label && !is_audio_label
            }
            ContentItem::InputImage { .. }
            | ContentItem::InputAudio { .. }
            | ContentItem::OutputText { .. } => true,
        })
}

/// Reconstructs user input after the caller has excluded contextual messages.
/// Media labels are omitted only when adjacent to the corresponding media item.
pub fn user_message_input(message: &[ContentItem]) -> Vec<UserInput> {
    let mut content: Vec<UserInput> = Vec::new();

    for (_, content_item) in user_input_parts(message) {
        match content_item {
            ContentItem::InputText { text } => {
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
