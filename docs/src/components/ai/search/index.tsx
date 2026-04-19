"use client";

import {
  type ComponentProps,
  type ReactNode,
  type SyntheticEvent,
  createContext,
  use,
  useEffect,
  useEffectEvent,
  useMemo,
  useRef,
  useState,
} from "react";

import { type UseChatHelpers, useChat } from "@ai-sdk/react";
import { Presence } from "@radix-ui/react-presence";
import { DefaultChatTransport, type Tool, type UIToolInvocation } from "ai";
import {
  Loader2,
  MessageCircleIcon,
  RefreshCw,
  SearchIcon,
  Send,
  X,
} from "lucide-react";

import type { ChatUIMessage, SearchTool } from "../../../app/api/chat/route";
import { cn } from "../../../lib/cn";
import { Markdown } from "../../markdown";
import { buttonVariants } from "./components/button";

const Context = createContext<{
  open: boolean;
  setOpen: (open: boolean) => void;
  chat: UseChatHelpers<ChatUIMessage>;
} | null>(null);

export function AISearchPanelHeader({
  className,
  ...props
}: ComponentProps<"div">) {
  const { setOpen } = useAISearchContext();

  return (
    <div
      className={cn(
        "sticky top-0 flex items-start gap-2 rounded-[1px] border bg-fd-secondary text-fd-secondary-foreground shadow-sm",
        className
      )}
      {...props}
    >
      <div className="flex-1 px-3 py-2">
        <p className="mb-2 font-medium text-sm">AI Chat</p>
        <p className="text-fd-muted-foreground text-xs">
          AI can be inaccurate, please verify the answers.
        </p>
      </div>

      <button
        aria-label="Close"
        className={cn(
          buttonVariants({
            size: "icon-sm",
            color: "ghost",
            className: "rounded-none text-fd-muted-foreground",
          })
        )}
        onClick={() => setOpen(false)}
        tabIndex={-1}
        type="button"
      >
        <X />
      </button>
    </div>
  );
}

export function AISearchInputActions() {
  const { messages, status, setMessages, regenerate } = useChatContext();
  const isLoading = status === "streaming";

  if (messages.length === 0) {
    return null;
  }

  return (
    <>
      {!isLoading && messages.at(-1)?.role === "assistant" && (
        <button
          className={cn(
            buttonVariants({
              color: "secondary",
              size: "sm",
              className: "gap-1.5 rounded-none",
            })
          )}
          onClick={() => regenerate()}
          type="button"
        >
          <RefreshCw className="size-4" />
          Retry
        </button>
      )}
      <button
        className={cn(
          buttonVariants({
            color: "secondary",
            size: "sm",
            className: "rounded-none",
          })
        )}
        onClick={() => setMessages([])}
        type="button"
      >
        Clear Chat
      </button>
    </>
  );
}

const StorageKeyInput = "__ai_search_input";
export function AISearchInput(props: ComponentProps<"form">) {
  const { status, sendMessage, stop } = useChatContext();
  const [input, setInput] = useState(
    () => localStorage.getItem(StorageKeyInput) ?? ""
  );
  const isLoading = status === "streaming" || status === "submitted";
  const onStart = (e?: SyntheticEvent) => {
    e?.preventDefault();
    const message = input.trim();
    if (message.length === 0) {
      return;
    }

    sendMessage({
      role: "user",
      parts: [
        {
          type: "data-client",
          data: {
            location: location.href,
          },
        },
        {
          type: "text",
          text: message,
        },
      ],
    }).catch(() => {
      // swallow: errors surface through the chat's error state
    });
    setInput("");
    localStorage.removeItem(StorageKeyInput);
  };

  useEffect(() => {
    if (isLoading) {
      document.getElementById("nd-ai-input")?.focus();
    }
  }, [isLoading]);

  return (
    <form
      {...props}
      className={cn("flex items-start pe-2", props.className)}
      onSubmit={onStart}
    >
      <Input
        autoFocus
        className="p-3"
        disabled={status === "streaming" || status === "submitted"}
        onChange={(e) => {
          setInput(e.target.value);
          localStorage.setItem(StorageKeyInput, e.target.value);
        }}
        onKeyDown={(event) => {
          if (!event.shiftKey && event.key === "Enter") {
            onStart(event);
          }
        }}
        placeholder={isLoading ? "AI is answering..." : "Ask a question"}
        value={input}
      />
      {isLoading ? (
        <button
          className={cn(
            buttonVariants({
              color: "secondary",
              className: "mt-2 gap-2 rounded-none transition-all",
            })
          )}
          key="bn"
          onClick={stop}
          type="button"
        >
          <Loader2 className="size-4 animate-spin text-fd-muted-foreground" />
          Abort Answer
        </button>
      ) : (
        <button
          className={cn(
            buttonVariants({
              color: "primary",
              className: "mt-2 rounded-none transition-all",
            })
          )}
          disabled={input.length === 0}
          key="bn"
          type="submit"
        >
          <Send className="size-4" />
        </button>
      )}
    </form>
  );
}

function List(props: Omit<ComponentProps<"div">, "dir">) {
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!containerRef.current) {
      return;
    }
    function callback() {
      const container = containerRef.current;
      if (!container) {
        return;
      }

      container.scrollTo({
        top: container.scrollHeight,
        behavior: "instant",
      });
    }

    const observer = new ResizeObserver(callback);
    callback();

    const element = containerRef.current?.firstElementChild;

    if (element) {
      observer.observe(element);
    }

    return () => {
      observer.disconnect();
    };
  }, []);

  return (
    <div
      ref={containerRef}
      {...props}
      className={cn(
        "fd-scroll-container flex min-w-0 flex-col overflow-y-auto",
        props.className
      )}
    >
      {props.children}
    </div>
  );
}

function Input(props: ComponentProps<"textarea">) {
  const ref = useRef<HTMLDivElement>(null);
  const shared = cn("col-start-1 row-start-1", props.className);

  return (
    <div className="grid flex-1">
      <textarea
        id="nd-ai-input"
        {...props}
        className={cn(
          "resize-none bg-transparent placeholder:text-fd-muted-foreground focus-visible:outline-none",
          shared
        )}
      />
      <div className={cn(shared, "invisible break-all")} ref={ref}>
        {`${props.value?.toString() ?? ""}\n`}
      </div>
    </div>
  );
}

const roleName: Record<string, string> = {
  user: "you",
  assistant: "fumadocs",
};

function Message({
  message,
  ...props
}: { message: ChatUIMessage } & ComponentProps<"div">) {
  let markdown = "";
  const searchCalls: UIToolInvocation<SearchTool>[] = [];

  for (const part of message.parts ?? []) {
    if (part.type === "text") {
      markdown += part.text;
      continue;
    }

    if (part.type.startsWith("tool-")) {
      const toolName = part.type.slice("tool-".length);
      const p = part as UIToolInvocation<Tool>;

      if (toolName !== "search" || !p.toolCallId) {
        continue;
      }
      searchCalls.push(p);
    }
  }

  return (
    // biome-ignore lint/a11y/noStaticElementInteractions: wrapper stops click propagation to prevent closing the panel when a message is selected
    // biome-ignore lint/a11y/useKeyWithClickEvents: no keyboard equivalent needed, click is a mouse-only stopPropagation handler
    // biome-ignore lint/a11y/noNoninteractiveElementInteractions: div is a layout wrapper, interactivity belongs to descendant buttons and links
    <div onClick={(e) => e.stopPropagation()} {...props}>
      <p
        className={cn(
          "mb-1 font-medium text-fd-muted-foreground text-sm",
          message.role === "assistant" && "text-fd-primary"
        )}
      >
        {roleName[message.role] ?? "unknown"}
      </p>
      <div className="prose text-sm">
        <Markdown text={markdown} />
      </div>

      {searchCalls.map((call) => {
        return (
          <div
            className="mt-3 flex flex-row items-center gap-2 rounded-[1px] border bg-fd-secondary p-2 text-fd-muted-foreground text-xs"
            key={call.toolCallId}
          >
            <SearchIcon className="size-4" />
            {call.state === "output-error" || call.state === "output-denied" ? (
              <p className="text-fd-error">
                {call.errorText ?? "Failed to search"}
              </p>
            ) : (
              <p>
                {call.output
                  ? `${call.output.length} search results`
                  : "Searching…"}
              </p>
            )}
          </div>
        );
      })}
    </div>
  );
}

export function AISearch({ children }: { children: ReactNode }) {
  const [open, setOpen] = useState(false);
  const chat = useChat<ChatUIMessage>({
    id: "search",
    transport: new DefaultChatTransport({
      api: "/api/chat",
    }),
  });

  return (
    <Context value={useMemo(() => ({ chat, open, setOpen }), [chat, open])}>
      {children}
    </Context>
  );
}

export function AISearchTrigger({
  position = "default",
  className,
  ...props
}: ComponentProps<"button"> & { position?: "default" | "float" }) {
  const { open, setOpen } = useAISearchContext();

  return (
    <button
      className={cn(
        position === "float" && [
          "fixed inset-e-[calc(--spacing(4)+var(--removed-body-scroll-bar-size,0px))] bottom-4 z-20 w-24 gap-3 shadow-lg transition-[translate,opacity]",
          open && "translate-y-10 opacity-0",
        ],
        className
      )}
      data-state={open ? "open" : "closed"}
      onClick={() => setOpen(!open)}
      {...props}
    >
      {props.children}
    </button>
  );
}

export function AISearchPanel() {
  const { open, setOpen } = useAISearchContext();
  useHotKey();

  return (
    <>
      <style>
        {`
        @keyframes ask-ai-open {
          from {
            translate: 100% 0;
          }
          to {
            translate: 0 0;
          }
        }
        @keyframes ask-ai-close {
          from {
            width: var(--ai-chat-width);
          }
          to {
            width: 0px;
          }
        }`}
      </style>
      <Presence present={open}>
        {/* biome-ignore lint/a11y/noStaticElementInteractions: backdrop, closing via Escape is handled globally in useHotKey */}
        {/* biome-ignore lint/a11y/useKeyWithClickEvents: backdrop, Escape handler is in useHotKey */}
        {/* biome-ignore lint/a11y/noNoninteractiveElementInteractions: decorative backdrop, click-outside-to-dismiss pattern */}
        <div
          className="fixed inset-0 z-30 bg-fd-overlay backdrop-blur-xs data-[state=closed]:animate-fd-fade-out data-[state=open]:animate-fd-fade-in lg:hidden"
          data-state={open ? "open" : "closed"}
          onClick={() => setOpen(false)}
        />
      </Presence>
      <Presence present={open}>
        <div
          className={cn(
            "z-30 overflow-hidden bg-fd-card text-fd-card-foreground [--ai-chat-width:400px] 2xl:[--ai-chat-width:460px]",
            "max-lg:fixed max-lg:inset-x-2 max-lg:inset-y-4 max-lg:rounded-[1px] max-lg:border max-lg:shadow-xl",
            "lg:sticky lg:top-0 lg:in-[#nd-notebook-layout]:col-start-5 lg:in-[#nd-notebook-layout]:row-span-full lg:ms-auto lg:h-dvh lg:border-s lg:in-[#nd-docs-layout]:[grid-area:toc]",
            open
              ? "animate-fd-dialog-in lg:animate-[ask-ai-open_200ms]"
              : "animate-fd-dialog-out lg:animate-[ask-ai-close_200ms]"
          )}
        >
          <div className="flex size-full flex-col p-2 lg:w-(--ai-chat-width) lg:p-3">
            <AISearchPanelHeader />
            <AISearchPanelList className="flex-1" />
            <div className="rounded-[1px] border bg-fd-secondary text-fd-secondary-foreground shadow-sm has-focus-visible:shadow-md">
              <AISearchInput />
              <div className="flex items-center gap-1.5 p-1 empty:hidden">
                <AISearchInputActions />
              </div>
            </div>
          </div>
        </div>
      </Presence>
    </>
  );
}

export function AISearchPanelList({
  className,
  style,
  ...props
}: ComponentProps<"div">) {
  const chat = useChatContext();
  const messages = chat.messages.filter((msg) => msg.role !== "system");

  return (
    <List
      className={cn("overscroll-contain py-4", className)}
      style={{
        maskImage:
          "linear-gradient(to bottom, transparent, white 1rem, white calc(100% - 1rem), transparent 100%)",
        ...style,
      }}
      {...props}
    >
      {messages.length === 0 ? (
        <div className="flex size-full flex-col items-center justify-center gap-2 text-center text-fd-muted-foreground/80 text-sm">
          <MessageCircleIcon fill="currentColor" stroke="none" />
          {/* biome-ignore lint/a11y/noNoninteractiveElementInteractions: stops clicks on the empty-state text from bubbling to the backdrop */}
          {/* biome-ignore lint/a11y/useKeyWithClickEvents: no keyboard equivalent needed, mouse-only stopPropagation */}
          <p onClick={(e) => e.stopPropagation()}>Start a new chat below.</p>
        </div>
      ) : (
        <div className="flex flex-col gap-4 px-3">
          {chat.error && (
            <div className="rounded-[1px] border bg-fd-secondary p-2 text-fd-secondary-foreground">
              <p className="mb-1 text-fd-muted-foreground text-xs">
                Request Failed: {chat.error.name}
              </p>
              <p className="text-sm">{chat.error.message}</p>
            </div>
          )}
          {messages.map((item) => (
            <Message key={item.id} message={item} />
          ))}
        </div>
      )}
    </List>
  );
}

export function useHotKey() {
  const { open, setOpen } = useAISearchContext();

  const onKeyPress = useEffectEvent((e: KeyboardEvent) => {
    if (e.key === "Escape" && open) {
      setOpen(false);
      e.preventDefault();
    }

    if (e.key === "/" && (e.metaKey || e.ctrlKey) && !open) {
      setOpen(true);
      e.preventDefault();
    }
  });

  useEffect(() => {
    window.addEventListener("keydown", onKeyPress);
    return () => window.removeEventListener("keydown", onKeyPress);
  }, []);
}

export function useAISearchContext() {
  const ctx = use(Context);
  if (!ctx) {
    throw new Error(
      "useAISearchContext must be used inside an AISearchProvider"
    );
  }
  return ctx;
}

function useChatContext() {
  const ctx = use(Context);
  if (!ctx) {
    throw new Error("useChatContext must be used inside an AISearchProvider");
  }
  return ctx.chat;
}
